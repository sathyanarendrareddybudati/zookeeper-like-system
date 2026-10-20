"""Process manager to launch and control the 5-node ZooKeeper cluster over raw TCP sockets.

Features:
- Launches Node 1 to Node 5 on localhost ports 5001 to 5005.
- Uses Python standard library sockets and threading.
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


def start_node(node_id: int, log_files: dict) -> subprocess.Popen:
    cfg = NODES[node_id]
    log_path = f"data/node_{node_id}.log"
    log_f = open(log_path, "a")
    log_files[node_id] = log_f
    cmd = [sys.executable, "node_server.py", "--id", str(node_id)]
    print(f"🚀 Launching Node {node_id} on TCP {cfg['host']}:{cfg['port']} (logs: {log_path})...")
    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc


def main():
    parser = argparse.ArgumentParser(description="Manage ZooKeeper 5-node cluster (TCP sockets)")
    parser.add_argument("--clean", action="store_true", help="Clean SQLite databases before launch")
    args = parser.parse_args()

    if args.clean:
        clean_data_dir()
    else:
        os.makedirs("data", exist_ok=True)

    processes = {}
    log_files = {}

    def shutdown(signum=None, frame=None):
        print("\n🛑 Shutting down all cluster nodes...")
        for nid, p in processes.items():
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()
        for f in log_files.values():
            try:
                f.close()
            except Exception:
                pass
        print("All nodes terminated. Goodbye!")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Launch all 5 nodes
    for nid in sorted(NODES.keys()):
        p = start_node(nid, log_files)
        processes[nid] = p
        time.sleep(0.3)  # Brief stagger for clean port binding

    print("\n" + "=" * 65)
    print("🌟 5-Node ZooKeeper Cluster Running (Raw TCP Sockets)!")
    print("   Node 1 (Initial Leader)")
    print("   Node 2 (Follower)")
    print("   Node 3 (Follower)")
    print("   Node 4 (Follower)")
    print("   Node 5 (Follower)")
    print("   Communicate with any node via: python client.py --interactive")
    print("   Press Ctrl+C to terminate the cluster.")
    print("=" * 65 + "\n")

    try:
        while True:
            # Check if any process died unexpectedly
            for nid, p in list(processes.items()):
                ret = p.poll()
                if ret is not None:
                    log_path = f"data/node_{nid}.log"
                    out = ""
                    if os.path.exists(log_path):
                        with open(log_path, "r") as f:
                            out = "".join(f.readlines()[-15:])
                    print(f"⚠️ Node {nid} exited with code {ret}.\n{out}")
                    del processes[nid]
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()
