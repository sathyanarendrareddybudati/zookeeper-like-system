"""ElectionManager component for ZooKeeper-like System Lab.

Implements ZAB/Raft-style leader election:
- Periodic heartbeat monitoring.
- Triggers election on heartbeat timeout.
- Compares (epoch, seq); ties broken by lowest node ID.
- Synchronizes uncommitted log entries on new leader election.
- Increments epoch and resets seq to 0 upon winning election.
"""

import asyncio
import logging
import random
import time
from typing import Dict, Any, Optional
import httpx

from config import (
    NODES,
    QUORUM,
    HEARTBEAT_INTERVAL,
    HEARTBEAT_TIMEOUT_MIN,
    HEARTBEAT_TIMEOUT_MAX,
    REQUEST_TIMEOUT,
    INITIAL_LEADER_ID,
)
from log_store import LogStore

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
        self.voted_for_in_epoch: Dict[int, int] = {}
        self.is_running = False
        self._loop_task: Optional[asyncio.Task] = None

    def _get_random_timeout(self) -> float:
        """Randomize heartbeat timeout to prevent split votes."""
        return random.uniform(HEARTBEAT_TIMEOUT_MIN, HEARTBEAT_TIMEOUT_MAX)

    def start(self) -> None:
        """Start heartbeat monitoring & sending background loop."""
        if not self.is_running:
            self.is_running = True
            self.last_heartbeat_time = time.time()
            self._loop_task = asyncio.create_task(self._main_loop())
            logger.info(
                f"[Node {self.node_id}] Started ElectionManager as {self.role} "
                f"(timeout: {self.heartbeat_timeout:.2f}s)"
            )

    def stop(self) -> None:
        """Stop background loop."""
        self.is_running = False
        if self._loop_task:
            self._loop_task.cancel()

    async def _main_loop(self) -> None:
        """Main periodic loop for heartbeats and timeout checks."""
        while self.is_running:
            try:
                if self.role == "LEADER":
                    await self._send_heartbeats()
                    await asyncio.sleep(HEARTBEAT_INTERVAL)
                else:
                    # Follower or Candidate: check heartbeat timeout
                    elapsed = time.time() - self.last_heartbeat_time
                    if elapsed > self.heartbeat_timeout:
                        logger.warning(
                            f"[Node {self.node_id}] Heartbeat timeout expired ({elapsed:.2f}s > "
                            f"{self.heartbeat_timeout:.2f}s). Starting election!"
                        )
                        await self.start_election()
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Node {self.node_id}] Error in ElectionManager loop: {e}")
                await asyncio.sleep(1.0)

    async def _send_heartbeats(self) -> None:
        """Leader sends periodic heartbeats to all followers."""
        msg = {
            "epoch": self.epoch,
            "from_node": self.node_id,
        }
        peers = [nid for nid in NODES if nid != self.node_id]

        async def send_hb(peer_id: int):
            peer = NODES[peer_id]
            url = f"http://{peer['host']}:{peer['port']}/heartbeat"
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    await client.post(url, json=msg)
            except Exception:
                pass  # Followers may be offline, normal in distributed systems

        await asyncio.gather(*[send_hb(nid) for nid in peers], return_exceptions=True)

    def handle_heartbeat(self, epoch: int, from_node: int) -> Dict[str, Any]:
        """Process incoming HEARTBEAT from leader."""
        if epoch >= self.epoch:
            should_catchup = (self.role != "FOLLOWER" or self.current_leader != from_node or epoch > self.epoch)
            if should_catchup:
                logger.info(
                    f"[Node {self.node_id}] Acknowledging leader Node {from_node} for epoch {epoch} "
                    f"(previous role: {self.role}, previous epoch: {self.epoch})"
                )
                # Catch up log entries from the new/updated leader
                asyncio.create_task(self._catchup_from_leader(from_node))

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

    async def _catchup_from_leader(self, leader_id: int) -> None:
        """Fetch latest transaction log from the leader to catch up missed commits."""
        try:
            peer = NODES[leader_id]
            url = f"http://{peer['host']}:{peer['port']}/log"
            async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    entries = resp.json().get("entries", [])
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
            # Grant vote
            logger.info(f"[Node {self.node_id}] Voted YES for candidate Node {cand_id}")
            self.last_heartbeat_time = time.time()  # Reset timeout on valid candidate
            return {"vote_granted": True, "voter_id": self.node_id}
        else:
            logger.info(f"[Node {self.node_id}] Voted NO for candidate Node {cand_id} (my state is higher)")
            return {"vote_granted": False, "voter_id": self.node_id}

    async def start_election(self) -> None:
        """Conduct election: broadcast latest (epoch, seq), collect votes, sync logs if won."""
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
            "epoch": cand_epoch,
            "seq": cand_seq,
            "from_node": self.node_id,
        }

        async def request_vote(peer_id: int) -> bool:
            peer = NODES[peer_id]
            url = f"http://{peer['host']}:{peer['port']}/election"
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    resp = await client.post(url, json=election_msg)
                    if resp.status_code == 200 and resp.json().get("vote_granted") is True:
                        return True
            except Exception:
                pass
            return False

        tasks = [asyncio.create_task(request_vote(nid)) for nid in peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if r is True:
                votes += 1

        logger.info(f"[Node {self.node_id}] Election result: received {votes}/{len(NODES)} votes (Quorum: {QUORUM})")

        if votes >= QUORUM:
            # Won the election!
            logger.info(f"*** [Node {self.node_id}] ELECTED NEW LEADER! ***")
            
            # Step 3: Synchronise uncommitted log entries across followers
            await self._synchronise_logs(peers)

            # Step 4: Increment epoch and reset seq to 0
            new_epoch = max(cand_epoch, self.epoch) + 1
            self.epoch = new_epoch
            self.role = "LEADER"
            self.current_leader = self.node_id
            self.coordinator.set_epoch_and_reset_seq(new_epoch)

            # Broadcast first heartbeat with new epoch
            await self._send_heartbeats()
            logger.info(
                f"[Node {self.node_id}] Resumed normal operation as LEADER with epoch={new_epoch}, seq=0"
            )
        else:
            logger.info(f"[Node {self.node_id}] Did not achieve quorum. Reverting to follower.")
            self.role = "FOLLOWER"

    async def _synchronise_logs(self, peers: list) -> None:
        """Fetch logs from all reachable followers, reconcile committed entries, and push sync."""
        logger.info(f"[Node {self.node_id}] Synchronising logs with followers before resuming operation...")
        all_logs = {self.node_id: self.log_store.get_all_entries()}

        async def fetch_log(peer_id: int):
            peer = NODES[peer_id]
            url = f"http://{peer['host']}:{peer['port']}/log"
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        return peer_id, resp.json().get("entries", [])
            except Exception:
                pass
            return peer_id, []

        results = await asyncio.gather(*[fetch_log(nid) for nid in peers], return_exceptions=True)
        for res in results:
            if isinstance(res, tuple):
                pid, entries = res
                all_logs[pid] = entries

        # Count occurrences of entries: if an entry is COMMITTED anywhere or proposed by quorum, commit it
        committed_entries = {}
        for pid, entries in all_logs.items():
            for e in entries:
                key = (e["epoch"], e["seq"])
                if key not in committed_entries or e["status"] == "COMMITTED":
                    committed_entries[key] = dict(e)

        merged_list = list(committed_entries.values())
        self.log_store.sync_entries(merged_list)

        # Push reconciled entries to all followers
        async def push_sync(peer_id: int):
            peer = NODES[peer_id]
            url = f"http://{peer['host']}:{peer['port']}/sync_logs"
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    await client.post(url, json={"entries": merged_list})
            except Exception:
                pass

        await asyncio.gather(*[push_sync(nid) for nid in peers], return_exceptions=True)
        logger.info(f"[Node {self.node_id}] Log synchronisation completed with {len(merged_list)} entries.")
