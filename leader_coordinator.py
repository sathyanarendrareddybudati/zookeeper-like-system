"""Leader Coordinator component for ZooKeeper-like System Lab.

Responsibilities:
- Assigns proposal tags (epoch, seq).
- Sends proposals to followers.
- Collects ACKs from followers.
- Broadcasts COMMIT once quorum is reached.
- Manages game state (target, round guesses, closest guess selection).
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
import httpx

from config import NODES, QUORUM, ACK_TIMEOUT, REQUEST_TIMEOUT
from log_store import LogStore

logger = logging.getLogger("LeaderCoordinator")


class LeaderCoordinator:
    def __init__(self, node_id: int, log_store: LogStore):
        self.node_id = node_id
        self.log_store = log_store
        self.epoch = 1
        self.seq = 0
        self.round_guesses: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()
        
        # Initialize seq from log store if available
        latest = self.log_store.get_latest_entry()
        if latest:
            self.epoch = latest["epoch"]
            self.seq = latest["seq"]

    def set_epoch_and_reset_seq(self, new_epoch: int) -> None:
        """Called when this node is elected new leader."""
        self.epoch = new_epoch
        self.seq = 0
        self.round_guesses.clear()
        logger.info(f"[Node {self.node_id}] Coordinator initialized with epoch={self.epoch}, seq={self.seq}")

    def get_current_target(self) -> Optional[int]:
        """Fetch the latest committed target value."""
        latest = self.log_store.get_latest_committed()
        return latest["value"] if latest else None

    async def propose_and_commit(self, op: str, value: int) -> Dict[str, Any]:
        """Execute full proposal -> ACK -> COMMIT cycle (ZAB consensus)."""
        async with self._lock:
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
                "epoch": proposal_epoch,
                "seq": proposal_seq,
                "op": op,
                "value": value,
                "from_node": self.node_id,
            }

            followers = [nid for nid in NODES if nid != self.node_id]
            ack_count = 1  # Leader itself counts as 1 ACK

            async def send_propose(peer_id: int) -> bool:
                peer = NODES[peer_id]
                url = f"http://{peer['host']}:{peer['port']}/propose"
                try:
                    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                        resp = await client.post(url, json=proposal_msg)
                        if resp.status_code == 200:
                            data = resp.json()
                            if data.get("ack") is True:
                                logger.info(
                                    f"[Node {self.node_id} (Leader)] Received ACK from Node {peer_id} "
                                    f"for (epoch={proposal_epoch}, seq={proposal_seq})"
                                )
                                return True
                except Exception as e:
                    logger.debug(f"[Node {self.node_id}] Failed to send PROPOSE to Node {peer_id}: {e}")
                return False

            # Wait for ACKs with ACK_TIMEOUT
            tasks = [asyncio.create_task(send_propose(nid)) for nid in followers]
            try:
                done, pending = await asyncio.wait(
                    tasks,
                    timeout=ACK_TIMEOUT,
                    return_when=asyncio.ALL_COMPLETED,
                )
                for task in done:
                    if task.result() is True:
                        ack_count += 1
            except Exception as e:
                logger.error(f"[Node {self.node_id}] Error waiting for ACKs: {e}")

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
                f"[Node {self.node_id} (Leader)] Quorum reached! COMMITTED locally (epoch={proposal_epoch}, seq={proposal_seq})"
            )

            # 4. Broadcast COMMIT message to followers
            commit_msg = {
                "epoch": proposal_epoch,
                "seq": proposal_seq,
                "from_node": self.node_id,
            }

            async def send_commit(peer_id: int):
                peer = NODES[peer_id]
                url = f"http://{peer['host']}:{peer['port']}/commit"
                try:
                    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                        await client.post(url, json=commit_msg)
                except Exception as e:
                    logger.debug(f"[Node {self.node_id}] Failed to send COMMIT to Node {peer_id}: {e}")

            await asyncio.gather(*[send_commit(nid) for nid in followers], return_exceptions=True)

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
        guess = {"client_id": client_id, "value": value}
        self.round_guesses.append(guess)
        logger.info(f"[Node {self.node_id} (Leader)] Recorded guess: {guess}")
        return {
            "recorded": True,
            "guess": guess,
            "total_guesses_in_round": len(self.round_guesses),
        }

    async def resolve_round(self) -> Dict[str, Any]:
        """Select closest guess to current target and commit as new target."""
        current_target = self.get_current_target()
        if current_target is None:
            return {"success": False, "error": "No target has been set yet. A Write is required first."}

        if not self.round_guesses:
            return {"success": False, "error": "No guesses submitted for this round."}

        # Find closest guess: min |guess - target|
        # Ties broken by earliest submitted guess
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
        commit_res = await self.propose_and_commit(op="GUESS", value=winning_value)

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
