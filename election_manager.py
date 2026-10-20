"""ElectionManager component for ZooKeeper-like System Lab.

Implements ZAB/Raft-style leader election using raw TCP sockets and threading:
- Periodic heartbeat monitoring.
- Triggers election on heartbeat timeout (randomized to prevent split votes).
- Compares (epoch, seq); ties broken by lowest node ID.
- Synchronizes uncommitted log entries on new leader election.
- Increments epoch and resets seq to 0 upon winning election.
"""

import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from config import (
    HEARTBEAT_INTERVAL,
    HEARTBEAT_TIMEOUT_MAX,
    HEARTBEAT_TIMEOUT_MIN,
    INITIAL_LEADER_ID,
    NODES,
    QUORUM,
    REQUEST_TIMEOUT,
)
from log_store import LogStore
from network_utils import send_rpc

logger = logging.getLogger("ElectionManager")


class ElectionManager:
    def __init__(self, node_id: int, log_store: LogStore, coordinator):
        self.node_id = node_id
        self.log_store = log_store
        self.coordinator = coordinator

        # Initial state
        if self.node_id == INITIAL_LEADER_ID:
            self.role = "LEADER"
            self.current_leader: Optional[int] = self.node_id
            self.epoch = 1
        else:
            self.role = "FOLLOWER"
            self.current_leader: Optional[int] = INITIAL_LEADER_ID
            self.epoch = 1

        self.last_heartbeat_time = time.time()
        self.heartbeat_timeout = self._get_random_timeout()
        self.is_running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def _get_random_timeout(self) -> float:
        """Randomize heartbeat timeout to prevent split votes."""
        return random.uniform(HEARTBEAT_TIMEOUT_MIN, HEARTBEAT_TIMEOUT_MAX)

    def start(self) -> None:
        """Start heartbeat monitoring & sending background thread."""
        with self._lock:
            if not self.is_running:
                self.is_running = True
                self.last_heartbeat_time = time.time()
                self._thread = threading.Thread(target=self._main_loop, daemon=True)
                self._thread.start()
                logger.info(
                    f"[Node {self.node_id}] Started ElectionManager as {self.role} "
                    f"(timeout: {self.heartbeat_timeout:.2f}s)"
                )

    def stop(self) -> None:
        """Stop background thread."""
        with self._lock:
            self.is_running = False

    def _main_loop(self) -> None:
        """Main periodic loop for heartbeats and timeout checks."""
        while self.is_running:
            try:
                with self._lock:
                    current_role = self.role

                if current_role == "LEADER":
                    self._send_heartbeats()
                    time.sleep(HEARTBEAT_INTERVAL)
                else:
                    # Follower or Candidate: check heartbeat timeout
                    elapsed = time.time() - self.last_heartbeat_time
                    if elapsed > self.heartbeat_timeout:
                        logger.warning(
                            f"[Node {self.node_id}] Heartbeat timeout expired ({elapsed:.2f}s > "
                            f"{self.heartbeat_timeout:.2f}s). Starting election!"
                        )
                        self.start_election()
                    time.sleep(0.3)
            except Exception as e:
                logger.error(f"[Node {self.node_id}] Error in ElectionManager loop: {e}")
                time.sleep(0.5)

    def _send_heartbeats(self) -> None:
        """Leader sends periodic heartbeats to all followers over TCP sockets."""
        with self._lock:
            msg = {
                "type": "HEARTBEAT",
                "epoch": self.epoch,
                "from": self.node_id,
            }

        peers = [nid for nid in NODES if nid != self.node_id]

        def send_hb(peer_id: int):
            peer = NODES[peer_id]
            send_rpc(peer["host"], peer["port"], msg, timeout=REQUEST_TIMEOUT)

        if peers:
            with ThreadPoolExecutor(max_workers=len(peers)) as executor:
                for nid in peers:
                    executor.submit(send_hb, nid)

    def handle_heartbeat(self, epoch: int, from_node: int) -> Dict[str, Any]:
        """Process incoming HEARTBEAT from leader."""
        with self._lock:
            if epoch >= self.epoch:
                should_catchup = (
                    self.role != "FOLLOWER"
                    or self.current_leader != from_node
                    or epoch > self.epoch
                )
                if should_catchup:
                    logger.info(
                        f"[Node {self.node_id}] Acknowledging leader Node {from_node} for epoch {epoch} "
                        f"(previous role: {self.role}, previous epoch: {self.epoch})"
                    )
                    threading.Thread(
                        target=self._catchup_from_leader,
                        args=(from_node,),
                        daemon=True,
                    ).start()

                self.role = "FOLLOWER"
                self.current_leader = from_node
                self.epoch = epoch
                self.last_heartbeat_time = time.time()
                self.heartbeat_timeout = self._get_random_timeout()
                return {"status": "ok", "ack": True}
            else:
                logger.warning(
                    f"[Node {self.node_id}] Rejected HEARTBEAT from stale leader Node {from_node} "
                    f"(their epoch: {epoch}, my epoch: {self.epoch})"
                )
                return {"status": "stale_leader", "epoch": self.epoch}

    def _catchup_from_leader(self, leader_id: int) -> None:
        """Fetch latest transaction log from the leader to catch up missed commits."""
        try:
            peer = NODES[leader_id]
            msg = {"type": "GET_LOGS"}
            resp = send_rpc(peer["host"], peer["port"], msg, timeout=REQUEST_TIMEOUT)
            if resp and "entries" in resp:
                entries = resp.get("entries", [])
                if entries:
                    self.log_store.sync_entries(entries)
                    logger.info(
                        f"[Node {self.node_id}] Synchronised {len(entries)} entries from Leader Node {leader_id}"
                    )
        except Exception as e:
            logger.debug(f"[Node {self.node_id}] Catchup error from Node {leader_id}: {e}")

    def handle_election_request(self, cand_epoch: int, cand_seq: int, cand_id: int) -> Dict[str, Any]:
        """Evaluate incoming ELECTION message and vote according to ZAB rules:
        - Higher (epoch, seq) wins.
        - Ties broken by lowest node ID.
        """
        my_latest = self.log_store.get_latest_entry()
        my_epoch = my_latest["epoch"] if my_latest else self.epoch
        my_seq = my_latest["seq"] if my_latest else 0

        logger.info(
            f"[Node {self.node_id}] Received ELECTION from Node {cand_id} with (epoch={cand_epoch}, seq={cand_seq}). "
            f"My log: (epoch={my_epoch}, seq={my_seq})"
        )

        cand_tuple = (cand_epoch, cand_seq, -cand_id)
        my_tuple = (my_epoch, my_seq, -self.node_id)

        # Candidate is at least as up-to-date as this node
        if cand_tuple >= my_tuple:
            logger.info(f"[Node {self.node_id}] Voted YES for candidate Node {cand_id}")
            self.last_heartbeat_time = time.time()  # Reset timeout on valid candidate
            return {"type": "VOTE", "vote_granted": True, "voter_id": self.node_id}
        else:
            logger.info(f"[Node {self.node_id}] Voted NO for candidate Node {cand_id} (my state is higher)")
            return {"type": "VOTE", "vote_granted": False, "voter_id": self.node_id}

    def start_election(self) -> None:
        """Conduct election: broadcast latest (epoch, seq), collect votes, sync logs if won."""
        with self._lock:
            self.role = "CANDIDATE"
            self.current_leader = None
            self.heartbeat_timeout = self._get_random_timeout()
            self.last_heartbeat_time = time.time()

            my_latest = self.log_store.get_latest_entry()
            cand_epoch = my_latest["epoch"] if my_latest else self.epoch
            cand_seq = my_latest["seq"] if my_latest else 0

        votes = 1  # Vote for self
        logger.info(
            f"[Node {self.node_id}] Broadcast ELECTION: epoch={cand_epoch}, seq={cand_seq}, cand_id={self.node_id}"
        )

        peers = [nid for nid in NODES if nid != self.node_id]
        election_msg = {
            "type": "ELECTION",
            "epoch": cand_epoch,
            "seq": cand_seq,
            "from": self.node_id,
        }

        def request_vote(peer_id: int) -> bool:
            peer = NODES[peer_id]
            resp = send_rpc(peer["host"], peer["port"], election_msg, timeout=REQUEST_TIMEOUT)
            if resp and resp.get("vote_granted") is True:
                return True
            return False

        if peers:
            with ThreadPoolExecutor(max_workers=len(peers)) as executor:
                futures = [executor.submit(request_vote, nid) for nid in peers]
                for fut in as_completed(futures):
                    try:
                        if fut.result() is True:
                            votes += 1
                    except Exception:
                        pass

        logger.info(
            f"[Node {self.node_id}] Election result: received {votes}/{len(NODES)} votes (Quorum: {QUORUM})"
        )

        if votes >= QUORUM:
            logger.info(f"*** [Node {self.node_id}] ELECTED NEW LEADER! ***")

            # Step 3 (PDF): Synchronise uncommitted log entries across followers
            self._synchronise_logs(peers)

            with self._lock:
                # Step 4 (PDF): Increment epoch and reset seq to 0
                new_epoch = max(cand_epoch, self.epoch) + 1
                self.epoch = new_epoch
                self.role = "LEADER"
                self.current_leader = self.node_id
                self.coordinator.set_epoch_and_reset_seq(new_epoch)

            # Broadcast first heartbeat with new epoch
            self._send_heartbeats()
            logger.info(
                f"[Node {self.node_id}] Resumed normal operation as LEADER with epoch={new_epoch}, seq=0"
            )
        else:
            logger.info(f"[Node {self.node_id}] Did not achieve quorum. Reverting to follower.")
            with self._lock:
                self.role = "FOLLOWER"

    def _synchronise_logs(self, peers: List[int]) -> None:
        """Fetch logs from all reachable followers, reconcile committed entries, and push sync."""
        logger.info(f"[Node {self.node_id}] Synchronising logs with followers before resuming operation...")
        all_logs: Dict[int, List[Dict[str, Any]]] = {self.node_id: self.log_store.get_all_entries()}

        def fetch_log(peer_id: int):
            peer = NODES[peer_id]
            msg = {"type": "GET_LOGS"}
            resp = send_rpc(peer["host"], peer["port"], msg, timeout=REQUEST_TIMEOUT)
            if resp and "entries" in resp:
                return peer_id, resp.get("entries", [])
            return peer_id, []

        if peers:
            with ThreadPoolExecutor(max_workers=len(peers)) as executor:
                futures = [executor.submit(fetch_log, nid) for nid in peers]
                for fut in as_completed(futures):
                    try:
                        pid, entries = fut.result()
                        all_logs[pid] = entries
                    except Exception:
                        pass

        # Reconcile entries
        committed_entries: Dict[tuple, Dict[str, Any]] = {}
        for pid, entries in all_logs.items():
            for e in entries:
                key = (e["epoch"], e["seq"])
                if key not in committed_entries or e["status"] == "COMMITTED":
                    committed_entries[key] = dict(e)

        merged_list = list(committed_entries.values())
        self.log_store.sync_entries(merged_list)

        # Push reconciled entries to all followers
        sync_msg = {"type": "SYNC_LOGS", "entries": merged_list}

        def push_sync(peer_id: int):
            peer = NODES[peer_id]
            send_rpc(peer["host"], peer["port"], sync_msg, timeout=REQUEST_TIMEOUT)

        if peers:
            with ThreadPoolExecutor(max_workers=len(peers)) as executor:
                for nid in peers:
                    executor.submit(push_sync, nid)

        logger.info(f"[Node {self.node_id}] Log synchronisation completed with {len(merged_list)} entries.")
