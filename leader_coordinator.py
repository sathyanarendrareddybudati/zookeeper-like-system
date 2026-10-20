"""Leader Coordinator component for ZooKeeper-like System Lab.

Responsibilities:
- Assigns proposal tags (epoch, seq).
- Sends proposals to followers via raw TCP sockets.
- Collects ACKs from followers within ACK_TIMEOUT.
- Broadcasts COMMIT once quorum (majority) is reached.
- Manages game state (target, round guesses, closest guess selection).
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from config import ACK_TIMEOUT, NODES, QUORUM, REQUEST_TIMEOUT
from log_store import LogStore
from network_utils import send_rpc

logger = logging.getLogger("LeaderCoordinator")


class LeaderCoordinator:
    def __init__(self, node_id: int, log_store: LogStore):
        self.node_id = node_id
        self.log_store = log_store
        self.epoch = 1
        self.seq = 0
        self.round_guesses: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

        # Initialize seq and epoch from persistent log store if available
        latest = self.log_store.get_latest_entry()
        if latest:
            self.epoch = latest["epoch"]
            self.seq = latest["seq"]

    def set_epoch_and_reset_seq(self, new_epoch: int) -> None:
        """Called when this node is elected as the new leader."""
        with self._lock:
            self.epoch = new_epoch
            self.seq = 0
            self.round_guesses.clear()
            logger.info(
                f"[Node {self.node_id}] Coordinator initialized with epoch={self.epoch}, seq={self.seq}"
            )

    def get_current_target(self) -> Optional[int]:
        """Fetch the latest committed target value."""
        latest = self.log_store.get_latest_committed()
        return latest["value"] if latest else None

    def propose_and_commit(self, op: str, value: int) -> Dict[str, Any]:
        """Execute full proposal -> ACK -> COMMIT cycle (ZAB consensus)."""
        with self._lock:
            self.seq += 1
            proposal_seq = self.seq
            proposal_epoch = self.epoch

            logger.info(
                f"[Node {self.node_id} (Leader)] Initiating proposal: epoch={proposal_epoch}, "
                f"seq={proposal_seq}, op={op}, value={value}"
            )

            # 1. Log proposal locally with status PROPOSED
            self.log_store.append(
                epoch=proposal_epoch,
                seq=proposal_seq,
                op=op,
                value=value,
                status="PROPOSED",
            )

            # 2. Broadcast PROPOSE message to followers
            proposal_msg = {
                "type": "PROPOSE",
                "epoch": proposal_epoch,
                "seq": proposal_seq,
                "op": op,
                "value": value,
                "from": self.node_id,
            }

            followers = [nid for nid in NODES if nid != self.node_id]
            ack_count = 1  # Leader itself counts as 1 ACK

            def send_propose(peer_id: int) -> bool:
                peer = NODES[peer_id]
                resp = send_rpc(peer["host"], peer["port"], proposal_msg, timeout=REQUEST_TIMEOUT)
                if resp and resp.get("ack") is True:
                    logger.info(
                        f"[Node {self.node_id} (Leader)] Received ACK from Node {peer_id} "
                        f"for (epoch={proposal_epoch}, seq={proposal_seq})"
                    )
                    return True
                return False

            # Wait for ACKs concurrently with ThreadPoolExecutor
            if followers:
                with ThreadPoolExecutor(max_workers=len(followers)) as executor:
                    futures = [executor.submit(send_propose, nid) for nid in followers]
                    for fut in as_completed(futures):
                        try:
                            if fut.result() is True:
                                ack_count += 1
                        except Exception as e:
                            logger.debug(f"[Node {self.node_id}] Error collecting ACK: {e}")

            logger.info(
                f"[Node {self.node_id} (Leader)] Collected {ack_count}/{len(NODES)} ACKs "
                f"(Quorum needed: {QUORUM})"
            )

            if ack_count < QUORUM:
                logger.warning(
                    f"[Node {self.node_id} (Leader)] Quorum not reached! "
                    f"Got {ack_count} ACKs, needed {QUORUM}."
                )
                return {
                    "success": False,
                    "error": f"Quorum not reached (got {ack_count}/{QUORUM} ACKs)",
                    "epoch": proposal_epoch,
                    "seq": proposal_seq,
                }

            # 3. Quorum reached: Commit locally
            self.log_store.commit(epoch=proposal_epoch, seq=proposal_seq)
            logger.info(
                f"[Node {self.node_id} (Leader)] Quorum reached! COMMITTED locally "
                f"(epoch={proposal_epoch}, seq={proposal_seq})"
            )

            # 4. Broadcast COMMIT message to followers
            commit_msg = {
                "type": "COMMIT",
                "epoch": proposal_epoch,
                "seq": proposal_seq,
                "from": self.node_id,
            }

            def send_commit(peer_id: int):
                peer = NODES[peer_id]
                send_rpc(peer["host"], peer["port"], commit_msg, timeout=REQUEST_TIMEOUT)

            if followers:
                with ThreadPoolExecutor(max_workers=len(followers)) as executor:
                    for nid in followers:
                        executor.submit(send_commit, nid)

            return {
                "success": True,
                "epoch": proposal_epoch,
                "seq": proposal_seq,
                "op": op,
                "value": value,
                "status": "COMMITTED",
                "ack_count": ack_count,
            }

    def record_guess(self, client_id: str, value: int) -> Dict[str, Any]:
        """Record a client guess for the current round."""
        with self._lock:
            guess = {"client_id": client_id, "value": value}
            self.round_guesses.append(guess)
            logger.info(f"[Node {self.node_id} (Leader)] Recorded guess: {guess}")
            return {
                "recorded": True,
                "guess": guess,
                "total_guesses_in_round": len(self.round_guesses),
            }

    def resolve_round(self) -> Dict[str, Any]:
        """Select closest guess to current target and commit as new target."""
        with self._lock:
            current_target = self.get_current_target()
            if current_target is None:
                return {
                    "success": False,
                    "error": "No target has been set yet. A Write is required first.",
                }

            if not self.round_guesses:
                return {"success": False, "error": "No guesses submitted for this round."}

            # Find closest guess: min |guess - target|; ties broken by earliest submission
            closest = min(self.round_guesses, key=lambda g: abs(g["value"] - current_target))
            winning_value = closest["value"]
            winner_id = closest["client_id"]
            difference = abs(winning_value - current_target)

            logger.info(
                f"[Node {self.node_id} (Leader)] Round evaluation: Target was {current_target}. "
                f"Winner: {winner_id} with guess {winning_value} (diff: {difference}). "
                f"All guesses: {self.round_guesses}"
            )

        # Propose winning guess through full consensus cycle
        commit_res = self.propose_and_commit(op="GUESS", value=winning_value)

        with self._lock:
            if commit_res.get("success"):
                old_guesses = list(self.round_guesses)
                self.round_guesses.clear()
                return {
                    "success": True,
                    "winner": winner_id,
                    "winning_guess": winning_value,
                    "old_target": current_target,
                    "new_target": winning_value,
                    "difference": difference,
                    "all_guesses": old_guesses,
                    "consensus": commit_res,
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to commit closest guess: {commit_res.get('error')}",
                }
