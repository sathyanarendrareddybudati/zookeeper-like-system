"""Automated validation of all three Testing Scenarios required by the lab.

Scenarios tested over raw TCP sockets:
1. Normal write flow: Client sets target -> leader proposes -> quorum ACKs -> COMMIT -> all nodes see target.
2. Guessing flow: Multiple clients submit guesses -> leader picks closest -> commits new target -> next round begins.
3. Leader crash and recovery: Kill leader process -> followers detect timeout -> new leader elected -> logs synchronised -> operations resume correctly.
"""

import os
import shutil
import subprocess
import sys
import time
from typing import Any, Dict

from config import NODES
from network_utils import send_rpc


class ClusterRunner:
    def __init__(self):
        self.processes: Dict[int, subprocess.Popen] = {}
        self.log_files: Dict[int, Any] = {}

    def clean_data(self):
        if os.path.exists("data"):
            shutil.rmtree("data")
        os.makedirs("data", exist_ok=True)

    def start_node(self, node_id: int):
        cmd = [sys.executable, "node_server.py", "--id", str(node_id)]
        log_f = open(f"data/node_{node_id}.log", "a")
        self.log_files[node_id] = log_f
        p = subprocess.Popen(
            cmd,
            stdout=log_f,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.processes[node_id] = p

    def start_all(self):
        print("--- [Setup] Starting all 5 cluster nodes (TCP sockets) ---")
        for nid in sorted(NODES.keys()):
            self.start_node(nid)
            time.sleep(0.3)
        self.wait_for_cluster()

    def wait_for_cluster(self, timeout=10):
        start = time.time()
        while time.time() - start < timeout:
            alive = 0
            for nid, cfg in NODES.items():
                if nid in self.processes:
                    ret = self.processes[nid].poll()
                    if ret is not None:
                        log_path = f"data/node_{nid}.log"
                        err_sample = ""
                        if os.path.exists(log_path):
                            with open(log_path, "r") as f:
                                err_sample = "".join(f.readlines()[-15:])
                        raise RuntimeError(f"Node {nid} exited unexpectedly (code {ret}):\n{err_sample}")
                    
                    data = send_rpc(cfg["host"], cfg["port"], {"type": "GET_STATUS"}, timeout=0.5)
                    if data and "role" in data:
                        alive += 1
            if alive == len(self.processes) and alive > 0:
                print(f"--- [Setup] All {alive} nodes are healthy and responding ---")
                return True
            time.sleep(0.5)
        raise RuntimeError("Cluster failed to start within timeout.")

    def kill_node(self, node_id: int):
        if node_id in self.processes:
            p = self.processes[node_id]
            print(f"💥 Killing Node {node_id} process (PID: {p.pid})...")
            p.terminate()
            try:
                p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill()
            del self.processes[node_id]
            if node_id in self.log_files:
                try:
                    self.log_files[node_id].close()
                except Exception:
                    pass
                del self.log_files[node_id]

    def stop_all(self):
        print("\n--- [Teardown] Stopping all nodes ---")
        for nid, p in list(self.processes.items()):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    p.kill()
        for f in self.log_files.values():
            try:
                f.close()
            except Exception:
                pass
        self.processes.clear()
        self.log_files.clear()


def run_tests():
    runner = ClusterRunner()
    runner.clean_data()

    try:
        runner.start_all()
        time.sleep(1.5)  # Allow initial leader heartbeats to establish

        # =========================================================================
        # SCENARIO 1: Normal Write Flow
        # =========================================================================
        print("\n" + "=" * 70)
        print("▶️ TEST SCENARIO 1: Normal Write Flow")
        print("   Target: 50 | Client connects to Follower Node 3 (tests forwarding)")
        print("=" * 70)

        # Send Write(50) to Follower Node 3
        node3_cfg = NODES[3]
        print(f"1. Client sending WRITE (value=50) to Follower Node 3 ({node3_cfg['host']}:{node3_cfg['port']})...")
        write_msg = {"type": "WRITE", "value": 50, "client_id": "Client_Init"}
        resp = send_rpc(node3_cfg["host"], node3_cfg["port"], write_msg, timeout=5.0)
        assert resp is not None, "No response from Node 3"
        print(f"   Response: {resp}")
        assert resp.get("connected_role") == "FOLLOWER"
        assert resp.get("forwarded_to") == 1
        assert resp.get("result", {}).get("status") == "COMMITTED"

        # Verify all 5 nodes see the committed target 50
        print("2. Verifying committed target across all 5 nodes via TCP sockets...")
        time.sleep(0.5)
        for nid, cfg in NODES.items():
            t_data = send_rpc(cfg["host"], cfg["port"], {"type": "GET_TARGET"}, timeout=2.0)
            assert t_data is not None, f"Node {nid} did not respond"
            print(f"   Node {nid}: Target = {t_data.get('target')} (Epoch: {t_data.get('target_epoch')}, Seq: {t_data.get('target_seq')})")
            assert t_data.get("target") == 50, f"Node {nid} has wrong target: {t_data}"

        print("✅ SCENARIO 1 PASSED: Target successfully proposed, ACKed by quorum, committed, and visible across all nodes!")

        # =========================================================================
        # SCENARIO 2: Guessing Flow
        # =========================================================================
        print("\n" + "=" * 70)
        print("▶️ TEST SCENARIO 2: Guessing Flow")
        print("   Current Target: 50")
        print("   Clients submit guesses: Client_Alice -> 30, Client_Bob -> 62")
        print("   Expected Winner: Client_Bob (62 is closer to 50 than 30)")
        print("=" * 70)

        # Client A submits guess 30 via Node 2
        print("1. Client_Alice submits Guess(30) to Node 2...")
        guess_a = {"type": "GUESS", "value": 30, "client_id": "Client_Alice"}
        r_a = send_rpc(NODES[2]["host"], NODES[2]["port"], guess_a, timeout=5.0)
        assert r_a is not None and r_a.get("status") == "ok"
        print(f"   Node 2 response: {r_a}")

        # Client B submits guess 62 via Node 4
        print("2. Client_Bob submits Guess(62) to Node 4...")
        guess_b = {"type": "GUESS", "value": 62, "client_id": "Client_Bob"}
        r_b = send_rpc(NODES[4]["host"], NODES[4]["port"], guess_b, timeout=5.0)
        assert r_b is not None and r_b.get("status") == "ok"
        print(f"   Node 4 response: {r_b}")

        # Resolve round via Node 5 (tests follower forwarding of resolution)
        print("3. Client triggers round evaluation via Node 5...")
        resolve_msg = {"type": "RESOLVE_ROUND"}
        r_res = send_rpc(NODES[5]["host"], NODES[5]["port"], resolve_msg, timeout=5.0)
        assert r_res is not None and r_res.get("status") == "ok"
        print(f"   Evaluation response: {r_res}")
        res = r_res.get("result", {})
        assert res.get("winner") == "Client_Bob", f"Wrong winner: {res}"
        assert res.get("winning_guess") == 62
        assert res.get("new_target") == 62

        # Verify all nodes updated their target to 62
        print("4. Verifying new target (62) across all 5 nodes...")
        time.sleep(0.5)
        for nid, cfg in NODES.items():
            t_data = send_rpc(cfg["host"], cfg["port"], {"type": "GET_TARGET"}, timeout=2.0)
            assert t_data is not None
            print(f"   Node {nid}: Target = {t_data.get('target')}")
            assert t_data.get("target") == 62

        print("✅ SCENARIO 2 PASSED: Closest guess picked, proposed, committed by quorum, new round began!")

        # =========================================================================
        # SCENARIO 3: Leader Crash and Recovery
        # =========================================================================
        print("\n" + "=" * 70)
        print("▶️ TEST SCENARIO 3: Leader Crash and Recovery")
        print("   Current Leader: Node 1")
        print("   Action: Terminate Node 1 -> Followers detect heartbeat loss -> Elect new leader")
        print("=" * 70)

        # 1. Kill Node 1
        runner.kill_node(1)

        # 2. Wait for heartbeat timeout and election (~3-5s)
        print("1. Waiting for followers to detect timeout and elect new leader...")
        new_leader_id = None
        survivors = [2, 3, 4, 5]

        for _ in range(14):  # up to 7 seconds
            time.sleep(0.5)
            for nid in survivors:
                s = send_rpc(NODES[nid]["host"], NODES[nid]["port"], {"type": "GET_STATUS"}, timeout=0.5)
                if s and s.get("role") == "LEADER":
                    new_leader_id = nid
                    print(f"   🎉 Detected new leader: Node {new_leader_id} (Epoch: {s.get('epoch')}, Seq: {s.get('seq')})")
                    break
            if new_leader_id:
                break

        assert new_leader_id is not None, "Failed to elect a new leader!"
        assert new_leader_id != 1, "Dead node cannot be leader!"

        # 3. Verify survivors acknowledge new leader and higher epoch
        print(f"2. Verifying cluster consensus under new Leader Node {new_leader_id}...")
        for nid in survivors:
            s = send_rpc(NODES[nid]["host"], NODES[nid]["port"], {"type": "GET_STATUS"}, timeout=1.0)
            assert s is not None
            print(f"   Node {nid}: Role={s.get('role')}, Leader=Node {s.get('current_leader')}, Epoch={s.get('epoch')}")
            assert s.get("current_leader") == new_leader_id

        # 4. Resume operations: Client sends Write(85) to surviving node (Node 3)
        print("3. Resuming operations: Client sends Write(85) to Node 3 under new leader...")
        write2_msg = {"type": "WRITE", "value": 85, "client_id": "Client_PostRecovery"}
        r_write2 = send_rpc(NODES[3]["host"], NODES[3]["port"], write2_msg, timeout=5.0)
        assert r_write2 is not None and r_write2.get("status") == "ok"
        print(f"   Write response: {r_write2}")

        # 5. Verify all surviving nodes have new target 85
        print("4. Verifying target 85 across surviving nodes...")
        time.sleep(0.5)
        for nid in survivors:
            t_data = send_rpc(NODES[nid]["host"], NODES[nid]["port"], {"type": "GET_TARGET"}, timeout=1.0)
            assert t_data is not None
            print(f"   Node {nid}: Target = {t_data.get('target')}")
            assert t_data.get("target") == 85

        # 6. Restart Node 1 to test rejoin and log sync
        print("5. Restarting Node 1 to verify rejoin and state catchup...")
        runner.start_node(1)
        time.sleep(2.5)  # Wait for heartbeats from new leader to reach Node 1

        s_rejoin = send_rpc(NODES[1]["host"], NODES[1]["port"], {"type": "GET_STATUS"}, timeout=1.0)
        assert s_rejoin is not None
        print(f"   Node 1 rejoined status: Role={s_rejoin.get('role')}, Leader=Node {s_rejoin.get('current_leader')}, Target={s_rejoin.get('current_target')}")
        assert s_rejoin.get("role") == "FOLLOWER", "Rejoined node must be FOLLOWER!"
        assert s_rejoin.get("current_leader") == new_leader_id
        assert s_rejoin.get("current_target") == 85, f"Expected caught-up target 85, got {s_rejoin.get('current_target')}"

        print("\n" + "=" * 70)
        print("🎉 ALL THREE TESTING SCENARIOS PASSED WITH 100% SUCCESS! 🎉")
        print("=" * 70)

    finally:
        runner.stop_all()


if __name__ == "__main__":
    run_tests()
