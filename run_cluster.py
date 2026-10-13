"""Process manager to launch and control the 5-node ZooKeeper cluster.

Features:
- Launches Node 1 to Node 5 on ports 5001 to 5005.
- Supports clean startup by optionally wiping previous SQLite data.
- Handles clean shutdown (SIGINT / Ctrl+C) of all child processes.
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time

from config import NODES


def clean_data_dir():
    if os.path.exists("data"):
        print("🧹 Cleaning data directory (data/)...")
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)


def start_node(node_id: int) -> subprocess.Popen:
    cfg = NODES[node_id]
    cmd = [sys.executable, "node_server.py", "--id", str(node_id)]
    print(f"🚀 Launching Node {node_id} on http://{cfg['host']}:{cfg['port']}...")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    return proc


def main():
    parser = argparse.ArgumentParser(description="Manage ZooKeeper 5-node cluster")
    parser.add_argument("--clean", action="store_true", help="Clean SQLite databases before launch")
    args = parser.parse_args()

    if args.clean:
        clean_data_dir()
    else:
        os.makedirs("data", exist_ok=True)

    processes = {}

    def shutdown(signum=None, frame=None):
        print("\n🛑 Shutting down all cluster nodes...")
        for nid, p in processes.items():
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()
        print("All nodes terminated. Goodbye!")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Launch all 5 nodes
    for nid in sorted(NODES.keys()):
        p = start_node(nid)
        processes[nid] = p
        time.sleep(0.3)  # Brief stagger for clean port binding

    print("\n" + "=" * 65)
    print("🌟 5-Node Cluster Running Successfully!")
    print("   Node 1 (Initial Leader): http://127.0.0.1:5001")
    print("   Node 2 (Follower)      : http://127.0.0.1:5002")
    print("   Node 3 (Follower)      : http://127.0.0.1:5003")
    print("   Node 4 (Follower)      : http://127.0.0.1:5004")
    print("   Node 5 (Follower)      : http://127.0.0.1:5005")
    print("   Dashboard & Swagger API: http://127.0.0.1:5001/ or /docs")
    print("   Press Ctrl+C to terminate the cluster.")
    print("=" * 65 + "\n")

    try:
        while True:
            # Check if any process died unexpectedly
            for nid, p in list(processes.items()):
                ret = p.poll()
                if ret is not None:
                    out = p.stdout.read() if p.stdout else ""
                    print(f"⚠️ Node {nid} exited with code {ret}.\n{out}")
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
