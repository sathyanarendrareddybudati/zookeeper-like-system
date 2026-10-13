"""Automated validation of all three Testing Scenarios required by the lab.

Scenarios:
1. Normal write flow: Client sets target -> leader proposes -> quorum ACKs -> COMMIT -> all nodes see target.
2. Guessing flow: Multiple clients submit guesses -> leader picks closest -> commits new target -> next round begins.
3. Leader crash and recovery: Kill leader process -> followers detect timeout -> new leader elected -> logs synchronised -> operations resume correctly.
"""

import os
import shutil
import subprocess
import sys
import time
from typing import Dict
import requests

from config import NODES


class ClusterRunner:
    def __init__(self):
        self.processes: Dict[int, subprocess.Popen] = {}

    def clean_data(self):
        if os.path.exists("data"):
            shutil.rmtree("data")
        os.makedirs("data", exist_ok=True)

    def start_node(self, node_id: int):
        cmd = [sys.executable, "node_server.py", "--id", str(node_id)]
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.processes[node_id] = p

    def start_all(self):
        print("--- [Setup] Starting all 5 cluster nodes ---")
        for nid in sorted(NODES.keys()):
            self.start_node(nid)
            time.sleep(0.3)
        # Wait for all nodes to become responsive
        self.wait_for_cluster()

    def wait_for_cluster(self, timeout=10):
        start = time.time()
        while time.time() - start < timeout:
            alive = 0
            for nid, cfg in NODES.items():
                if nid in self.processes and self.processes[nid].poll() is None:
                    try:
                        r = requests.get(f"http://{cfg['host']}:{cfg['port']}/status", timeout=0.5)
                        if r.status_code == 200:
                            alive += 1
                    except Exception:
                        pass
            if alive == len(self.processes):
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

    def stop_all(self):
        print("\n--- [Teardown] Stopping all nodes ---")
        for nid, p in list(self.processes.items()):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    p.kill()
        self.processes.clear()


def run_tests():
    runner = ClusterRunner()
    runner.clean_data()

    try:
        runner.start_all()
        time.sleep(1.5)  # Allow leader heartbeats to establish

        # =========================================================================
        # SCENARIO 1: Normal Write Flow
        # =========================================================================
        print("\n" + "=" * 70)
        print("▶️ TEST SCENARIO 1: Normal Write Flow")
        print("   Target: 50 | Client connects to Follower Node 3 (tests forwarding)")
        print("=" * 70)

        # Send Write(50) to Follower Node 3
        follower_url = f"http://{NODES[3]['host']}:{NODES[3]['port']}"
        print(f"1. Client sending POST /write (value=50) to Follower Node 3 ({follower_url})...")
        resp = requests.post(
            f"{follower_url}/write",
            json={"value": 50, "client_id": "Client_Init"},
            timeout=5.0,
        )
        assert resp.status_code == 200, f"Write failed: {resp.text}"
        data = resp.json()
        print(f"   Response: {data}")
        assert data.get("role") == "FOLLOWER"
        assert data.get("forwarded_to") == 1

        # Verify all 5 nodes see the committed target 50
        print("2. Verifying committed target across all 5 nodes...")
        time.sleep(0.5)
        for nid, cfg in NODES.items():
            r = requests.get(f"http://{cfg['host']}:{cfg['port']}/target", timeout=2.0)
            assert r.status_code == 200
            t_data = r.json()
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
        r_a = requests.post(
            f"http://{NODES[2]['host']}:{NODES[2]['port']}/guess",
            json={"value": 30, "client_id": "Client_Alice"},
            timeout=5.0,
        )
        assert r_a.status_code == 200
        print(f"   Node 2 response: {r_a.json()}")

        # Client B submits guess 62 via Node 4
        print("2. Client_Bob submits Guess(62) to Node 4...")
        r_b = requests.post(
            f"http://{NODES[4]['host']}:{NODES[4]['port']}/guess",
            json={"value": 62, "client_id": "Client_Bob"},
            timeout=5.0,
        )
        assert r_b.status_code == 200
        print(f"   Node 4 response: {r_b.json()}")

        # Resolve round via Node 5 (tests follower forwarding of resolution)
        print("3. Client triggers round evaluation via Node 5...")
        r_res = requests.post(
            f"http://{NODES[5]['host']}:{NODES[5]['port']}/resolve_round",
            timeout=5.0,
        )
        assert r_res.status_code == 200
        eval_data = r_res.json()
        print(f"   Evaluation response: {eval_data}")
        res = eval_data.get("result") or eval_data.get("response", {}).get("result", {})
        assert res.get("winner") == "Client_Bob", f"Wrong winner: {res}"
        assert res.get("winning_guess") == 62
        assert res.get("new_target") == 62

        # Verify all nodes updated their target to 62
        print("4. Verifying new target (62) across all 5 nodes...")
        time.sleep(0.5)
        for nid, cfg in NODES.items():
            r = requests.get(f"http://{cfg['host']}:{cfg['port']}/target", timeout=2.0)
            assert r.status_code == 200
            t_data = r.json()
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

        for _ in range(12):  # up to 6 seconds
            time.sleep(0.5)
            for nid in survivors:
                try:
                    r = requests.get(f"http://{NODES[nid]['host']}:{NODES[nid]['port']}/status", timeout=0.5)
                    if r.status_code == 200:
                        s = r.json()
                        if s.get("role") == "LEADER":
                            new_leader_id = nid
                            print(f"   🎉 Detected new leader: Node {new_leader_id} (Epoch: {s.get('epoch')}, Seq: {s.get('seq')})")
                            break
                except Exception:
                    pass
            if new_leader_id:
                break

        assert new_leader_id is not None, "Failed to elect a new leader!"
        assert new_leader_id != 1, "Dead node cannot be leader!"

        # 3. Verify survivors acknowledge new leader and higher epoch
        print(f"2. Verifying cluster consensus under new Leader Node {new_leader_id}...")
        for nid in survivors:
            r = requests.get(f"http://{NODES[nid]['host']}:{NODES[nid]['port']}/status", timeout=1.0)
            s = r.json()
            print(f"   Node {nid}: Role={s.get('role')}, Leader=Node {s.get('current_leader')}, Epoch={s.get('epoch')}")
            assert s.get("current_leader") == new_leader_id

        # 4. Resume operations: Client sends Write(85) to surviving node (Node 3)
        print("3. Resuming operations: Client sends Write(85) to Node 3 under new leader...")
        r_write2 = requests.post(
            f"http://{NODES[3]['host']}:{NODES[3]['port']}/write",
            json={"value": 85, "client_id": "Client_PostRecovery"},
            timeout=5.0,
        )
        assert r_write2.status_code == 200
        print(f"   Write response: {r_write2.json()}")

        # 5. Verify all surviving nodes have new target 85
        print("4. Verifying target 85 across surviving nodes...")
        time.sleep(0.5)
        for nid in survivors:
            r = requests.get(f"http://{NODES[nid]['host']}:{NODES[nid]['port']}/target", timeout=1.0)
            t_data = r.json()
            print(f"   Node {nid}: Target = {t_data.get('target')}")
            assert t_data.get("target") == 85

        # 6. Optional: Restart Node 1 to test rejoin and log sync
        print("5. Restarting Node 1 to verify rejoin and state catchup...")
        runner.start_node(1)
        time.sleep(2.5)  # Wait for heartbeats from new leader to reach Node 1

        r_rejoin = requests.get(f"http://{NODES[1]['host']}:{NODES[1]['port']}/status", timeout=1.0)
        s_rejoin = r_rejoin.json()
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
