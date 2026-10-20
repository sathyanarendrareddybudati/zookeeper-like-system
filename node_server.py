"""NodeServer implementation using standard Python TCP sockets and ThreadPoolExecutor.

Implements the ZooKeeper-like NodeServer interface:
- Accepts incoming TCP socket connections.
- Dispatches PROPOSE, ACK, COMMIT, HEARTBEAT, ELECTION, FORWARD messages.
- Handles client requests for Write(x), Guess(x), Target, Status, Log.
- Forwards client requests to the leader if this node is a follower.
- Pure Python standard library (no FastAPI, no external frameworks).
"""

import argparse
import json
import logging
import signal
import socket
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional

from config import NODES, REQUEST_TIMEOUT
from election_manager import ElectionManager
from leader_coordinator import LeaderCoordinator
from log_store import LogStore
from network_utils import send_rpc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("NodeServer")


class NodeServer:
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.cfg = NODES[node_id]
        self.host = self.cfg["host"]
        self.port = self.cfg["port"]

        self.log_store = LogStore(node_id=node_id)
        self.coordinator = LeaderCoordinator(node_id=node_id, log_store=self.log_store)
        self.election_manager = ElectionManager(
            node_id=node_id,
            log_store=self.log_store,
            coordinator=self.coordinator,
        )

        self.server_sock: Optional[socket.socket] = None
        self.is_running = False
        self.executor = ThreadPoolExecutor(max_workers=30)

    def start(self) -> None:
        """Bind TCP socket and start listening for connections."""
        self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(50)
        self.is_running = True

        logger.info(f"[Node {self.node_id}] Listening on raw TCP socket {self.host}:{self.port}")
        self.election_manager.start()

        try:
            while self.is_running:
                try:
                    conn, addr = self.server_sock.accept()
                    self.executor.submit(self._handle_connection, conn, addr)
                except socket.error:
                    if not self.is_running:
                        break
        finally:
            self.stop()

    def stop(self) -> None:
        """Shut down the server and clean up resources."""
        if not self.is_running:
            return
        self.is_running = False
        logger.info(f"[Node {self.node_id}] Shutting down NodeServer...")
        self.election_manager.stop()
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
        self.executor.shutdown(wait=False)

    def _handle_connection(self, conn: socket.socket, addr: Any) -> None:
        """Handle incoming connection, reading line-delimited JSON messages."""
        try:
            with conn:
                conn.settimeout(15.0)
                sock_file = conn.makefile("r", encoding="utf-8")
                while self.is_running:
                    line = sock_file.readline()
                    if not line:
                        break  # Connection closed by client
                    
                    line_str = line.strip()
                    if not line_str:
                        continue

                    try:
                        req = json.loads(line_str)
                    except json.JSONDecodeError:
                        err_resp = {"status": "error", "error": "Invalid JSON format"}
                        conn.sendall((json.dumps(err_resp) + "\n").encode("utf-8"))
                        continue

                    # Dispatch request and produce response
                    resp = self.dispatch_message(req)
                    conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))
        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            logger.debug(f"[Node {self.node_id}] Error handling connection from {addr}: {e}")

    def dispatch_message(self, req: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch incoming messages according to lab message protocol."""
        msg_type = req.get("type", "").upper()

        # 1. ZAB Consensus Messages
        if msg_type == "PROPOSE":
            # Follower logs proposal with status PROPOSED and replies ACK
            epoch = req["epoch"]
            seq = req["seq"]
            op = req["op"]
            value = req["value"]
            from_node = req.get("from", req.get("from_node"))

            logger.info(
                f"[Node {self.node_id}] Received PROPOSE from Node {from_node}: "
                f"epoch={epoch}, seq={seq}, op={op}, value={value}"
            )
            self.log_store.append(
                epoch=epoch,
                seq=seq,
                op=op,
                value=value,
                status="PROPOSED",
            )
            return {
                "type": "ACK",
                "epoch": epoch,
                "seq": seq,
                "from": self.node_id,
                "ack": True,
            }

        elif msg_type == "COMMIT":
            # Follower applies commit to local log, making new target visible
            epoch = req["epoch"]
            seq = req["seq"]
            logger.info(f"[Node {self.node_id}] Received COMMIT: epoch={epoch}, seq={seq}")
            success = self.log_store.commit(epoch=epoch, seq=seq)
            return {"status": "ok", "committed": success, "epoch": epoch, "seq": seq}

        elif msg_type == "HEARTBEAT":
            epoch = req["epoch"]
            from_node = req.get("from", req.get("from_node"))
            return self.election_manager.handle_heartbeat(epoch=epoch, from_node=from_node)

        elif msg_type == "ELECTION":
            epoch = req["epoch"]
            seq = req["seq"]
            from_node = req.get("from", req.get("from_node"))
            return self.election_manager.handle_election_request(
                cand_epoch=epoch,
                cand_seq=seq,
                cand_id=from_node,
            )

        elif msg_type == "FORWARD":
            # Leader receives forwarded request from follower
            op = req.get("op", "").upper()
            val = req.get("value")
            client_id = req.get("from_client", req.get("client_id", "anon"))
            resolve_now = req.get("resolve_immediately", False)

            logger.info(
                f"[Node {self.node_id} (Leader)] Received FORWARD: op={op}, val={val}, "
                f"client={client_id}"
            )

            if op == "WRITE":
                result = self.coordinator.propose_and_commit(op="WRITE", value=val)
                return {
                    "status": "ok",
                    "role": self.election_manager.role,
                    "handled_by": self.node_id,
                    "result": result,
                }
            elif op == "GUESS":
                guess_res = self.coordinator.record_guess(client_id=client_id, value=val)
                res_dict = {
                    "status": "ok",
                    "role": self.election_manager.role,
                    "handled_by": self.node_id,
                    "guess_result": guess_res,
                }
                if resolve_now:
                    res_dict["resolved"] = self.coordinator.resolve_round()
                return res_dict
            elif op == "RESOLVE_ROUND":
                return {
                    "status": "ok",
                    "role": self.election_manager.role,
                    "handled_by": self.node_id,
                    "result": self.coordinator.resolve_round(),
                }
            else:
                return {"status": "error", "error": f"Unknown forwarded op: {op}"}

        # 2. Client Operations (Write, Guess, Query)
        elif msg_type == "WRITE":
            val = req.get("value")
            client_id = req.get("client_id", "Client_Anon")

            if self.election_manager.role == "LEADER":
                res = self.coordinator.propose_and_commit(op="WRITE", value=val)
                return {
                    "status": "ok",
                    "role": "LEADER",
                    "handled_by": self.node_id,
                    "result": res,
                }
            else:
                # Forward to leader
                leader_id = self.election_manager.current_leader
                if not leader_id or leader_id not in NODES:
                    return {"status": "error", "error": "No leader currently available in cluster"}

                leader_cfg = NODES[leader_id]
                forward_msg = {
                    "type": "FORWARD",
                    "op": "WRITE",
                    "value": val,
                    "from_client": client_id,
                }
                resp = send_rpc(
                    leader_cfg["host"],
                    leader_cfg["port"],
                    forward_msg,
                    timeout=REQUEST_TIMEOUT * 2,
                )
                if resp:
                    resp["forwarded_by"] = self.node_id
                    resp["forwarded_to"] = leader_id
                    resp["connected_node"] = self.node_id
                    resp["connected_role"] = self.election_manager.role
                    return resp
                return {
                    "status": "error",
                    "error": f"Failed to forward WRITE to leader Node {leader_id}",
                }

        elif msg_type == "GUESS":
            val = req.get("value")
            client_id = req.get("client_id", "Client_Anon")
            resolve_now = req.get("resolve_immediately", False)

            if self.election_manager.role == "LEADER":
                guess_res = self.coordinator.record_guess(client_id=client_id, value=val)
                res_dict = {
                    "status": "ok",
                    "role": "LEADER",
                    "handled_by": self.node_id,
                    "guess_result": guess_res,
                }
                if resolve_now:
                    res_dict["resolved"] = self.coordinator.resolve_round()
                return res_dict
            else:
                # Forward to leader
                leader_id = self.election_manager.current_leader
                if not leader_id or leader_id not in NODES:
                    return {"status": "error", "error": "No leader currently available in cluster"}

                leader_cfg = NODES[leader_id]
                forward_msg = {
                    "type": "FORWARD",
                    "op": "GUESS",
                    "value": val,
                    "from_client": client_id,
                    "resolve_immediately": resolve_now,
                }
                resp = send_rpc(
                    leader_cfg["host"],
                    leader_cfg["port"],
                    forward_msg,
                    timeout=REQUEST_TIMEOUT * 2,
                )
                if resp:
                    resp["forwarded_by"] = self.node_id
                    resp["forwarded_to"] = leader_id
                    resp["connected_node"] = self.node_id
                    resp["connected_role"] = self.election_manager.role
                    return resp
                return {
                    "status": "error",
                    "error": f"Failed to forward GUESS to leader Node {leader_id}",
                }

        elif msg_type == "RESOLVE_ROUND":
            if self.election_manager.role == "LEADER":
                res = self.coordinator.resolve_round()
                return {
                    "status": "ok",
                    "role": "LEADER",
                    "handled_by": self.node_id,
                    "result": res,
                }
            else:
                leader_id = self.election_manager.current_leader
                if not leader_id or leader_id not in NODES:
                    return {"status": "error", "error": "No leader currently available in cluster"}
                leader_cfg = NODES[leader_id]
                forward_msg = {"type": "FORWARD", "op": "RESOLVE_ROUND"}
                resp = send_rpc(
                    leader_cfg["host"],
                    leader_cfg["port"],
                    forward_msg,
                    timeout=REQUEST_TIMEOUT * 2,
                )
                if resp:
                    resp["forwarded_by"] = self.node_id
                    resp["forwarded_to"] = leader_id
                    resp["connected_node"] = self.node_id
                    resp["connected_role"] = self.election_manager.role
                    return resp
                return {
                    "status": "error",
                    "error": f"Failed to forward RESOLVE_ROUND to leader Node {leader_id}",
                }

        elif msg_type == "GET_TARGET":
            latest_commit = self.log_store.get_latest_committed()
            return {
                "target": latest_commit["value"] if latest_commit else None,
                "target_epoch": latest_commit["epoch"] if latest_commit else None,
                "target_seq": latest_commit["seq"] if latest_commit else None,
            }

        elif msg_type == "GET_STATUS":
            latest = self.log_store.get_latest_entry()
            latest_commit = self.log_store.get_latest_committed()
            return {
                "node_id": self.node_id,
                "role": self.election_manager.role,
                "current_leader": self.election_manager.current_leader,
                "epoch": self.election_manager.epoch,
                "seq": latest["seq"] if latest else self.coordinator.seq,
                "current_target": latest_commit["value"] if latest_commit else None,
                "log_count": len(self.log_store.get_all_entries()),
            }

        elif msg_type == "GET_LOGS":
            return {"entries": self.log_store.get_all_entries()}

        elif msg_type == "SYNC_LOGS":
            entries = req.get("entries", [])
            self.log_store.sync_entries(entries)
            return {"status": "ok", "synced_count": len(entries)}

        else:
            return {"status": "error", "error": f"Unknown message type: {msg_type}"}


def main():
    parser = argparse.ArgumentParser(description="ZooKeeper-like Node Server (Raw TCP Sockets)")
    parser.add_argument("--id", type=int, required=True, choices=[1, 2, 3, 4, 5], help="Node ID (1-5)")
    args = parser.parse_args()

    node_id = args.id
    server = NodeServer(node_id=node_id)

    def handle_signal(sig, frame):
        logger.info(f"[Node {node_id}] Received signal {sig}, terminating...")
        server.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    server.start()


if __name__ == "__main__":
    main()
