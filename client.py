"""ClientApp component for ZooKeeper-like System Lab.

Allows clients to connect to ANY node in the cluster via raw TCP sockets:
- Send Write(x) to set the game target.
- Send Guess(x) to submit guesses.
- Trigger round evaluation to select closest guess.
- Query current target and cluster status.
- Supports both single-command CLI and interactive shell.
- Pure Python standard library (no requests, no external dependencies).
"""

import argparse
import sys
from typing import Optional

from config import NODES
from network_utils import send_rpc


def get_node_address(node_id: int) -> tuple:
    cfg = NODES.get(node_id, NODES[1])
    return cfg["host"], cfg["port"]


def send_write(host: str, port: int, value: int, client_id: str) -> None:
    msg = {"type": "WRITE", "value": value, "client_id": client_id}
    data = send_rpc(host, port, msg, timeout=5.0)
    if not data or data.get("status") == "error":
        print(f"❌ [Write Failed]: {data.get('error') if data else 'Connection timed out/refused'}")
        return

    print(f"✅ [Write Success]")
    print(f"   Target Set: {value}")
    print(f"   Connected Node: {data.get('handled_by')} ({data.get('role')})")
    if "forwarded_to" in data:
        print(f"   Forwarded to Leader: Node {data.get('forwarded_to')}")
    res = data.get("result", {})
    print(f"   Consensus: Epoch {res.get('epoch')}, Seq {res.get('seq')}, Status: {res.get('status')}")


def send_guess(
    host: str,
    port: int,
    value: int,
    client_id: str,
    resolve_immediately: bool = False,
) -> None:
    msg = {
        "type": "GUESS",
        "value": value,
        "client_id": client_id,
        "resolve_immediately": resolve_immediately,
    }
    data = send_rpc(host, port, msg, timeout=5.0)
    if not data or data.get("status") == "error":
        print(f"❌ [Guess Failed]: {data.get('error') if data else 'Connection timed out/refused'}")
        return

    print(f"✅ [Guess Submitted]")
    print(f"   Client: {client_id}")
    print(f"   Guess Value: {value}")
    print(f"   Connected Node: {data.get('handled_by')} ({data.get('role')})")
    if "forwarded_to" in data:
        print(f"   Forwarded to Leader: Node {data.get('forwarded_to')}")
    if data.get("resolved"):
        print(f"   Immediate Round Resolution: {data['resolved']}")


def trigger_resolve_round(host: str, port: int) -> None:
    msg = {"type": "RESOLVE_ROUND"}
    data = send_rpc(host, port, msg, timeout=5.0)
    if not data or data.get("status") == "error":
        print(f"❌ [Round Evaluation Failed]: {data.get('error') if data else 'Connection error'}")
        return

    print(f"🏆 [Round Evaluation Result]")
    res = data.get("result", {})
    if res.get("success"):
        print(f"   Winner: {res.get('winner')}")
        print(f"   Winning Guess: {res.get('winning_guess')}")
        print(f"   Old Target: {res.get('old_target')} -> New Target: {res.get('new_target')}")
        print(f"   Difference: {res.get('difference')}")
        print(f"   All Guesses Evaluated: {res.get('all_guesses')}")
        print(f"   Consensus Status: {res.get('consensus', {}).get('status')}")
    else:
        print(f"   Resolution Failed: {res.get('error')}")


def get_target(host: str, port: int) -> None:
    msg = {"type": "GET_TARGET"}
    data = send_rpc(host, port, msg, timeout=3.0)
    if not data:
        print(f"❌ [Get Target Failed]: Node did not respond.")
        return

    target = data.get("target")
    print(f"🎯 Current Game Target: {target if target is not None else 'NOT SET YET'}")
    if data.get("target_epoch") is not None:
        print(f"   Committed at: Epoch {data.get('target_epoch')}, Seq {data.get('target_seq')}")


def get_status(host: str, port: int) -> None:
    msg = {"type": "GET_STATUS"}
    data = send_rpc(host, port, msg, timeout=3.0)
    if not data:
        print(f"❌ [Get Status Failed]: Node did not respond.")
        return

    print(f"ℹ️ Node {data.get('node_id')} Status:")
    print(f"   Role: {data.get('role')}")
    print(f"   Current Leader: Node {data.get('current_leader')}")
    print(f"   Epoch: {data.get('epoch')}, Seq: {data.get('seq')}")
    print(f"   Current Target: {data.get('current_target')}")
    print(f"   Total Log Entries: {data.get('log_count')}")


def get_logs(host: str, port: int) -> None:
    msg = {"type": "GET_LOGS"}
    data = send_rpc(host, port, msg, timeout=3.0)
    if not data:
        print(f"❌ [Get Logs Failed]: Node did not respond.")
        return

    entries = data.get("entries", [])
    print(f"📜 Local Transaction Log ({len(entries)} entries):")
    print(f"   {'Epoch':<7} | {'Seq':<5} | {'Op':<6} | {'Value':<6} | {'Status':<10}")
    print("   " + "-" * 42)
    for e in entries:
        print(f"   {e['epoch']:<7} | {e['seq']:<5} | {e['op']:<6} | {e['value']:<6} | {e['status']:<10}")


def interactive_mode(node_id: int, client_id: str) -> None:
    current_node = node_id
    host, port = get_node_address(current_node)

    print("\n" + "=" * 60)
    print(f"🎮 ZooKeeper Game Client — Interactive Shell")
    print(f"   Connected to Node {current_node} ({host}:{port}) as '{client_id}'")
    print(f"   Commands: write <val>, guess <val>, resolve, target, status, logs, node <1-5>, exit")
    print("=" * 60 + "\n")

    while True:
        try:
            line = input(f"[{client_id} @ Node {current_node}]> ").strip()
            if not line:
                continue
            parts = line.split()
            cmd = parts[0].lower()

            if cmd in ("exit", "quit"):
                print("Exiting interactive shell.")
                break
            elif cmd == "node":
                if len(parts) > 1 and parts[1].isdigit() and int(parts[1]) in NODES:
                    current_node = int(parts[1])
                    host, port = get_node_address(current_node)
                    print(f"Switched connection to Node {current_node} ({host}:{port})")
                else:
                    print("Usage: node <1-5>")
            elif cmd == "write":
                if len(parts) > 1 and parts[1].isdigit():
                    send_write(host, port, int(parts[1]), client_id)
                else:
                    print("Usage: write <number 0-100>")
            elif cmd == "guess":
                if len(parts) > 1 and parts[1].isdigit():
                    send_guess(host, port, int(parts[1]), client_id)
                else:
                    print("Usage: guess <number 0-100>")
            elif cmd == "resolve":
                trigger_resolve_round(host, port)
            elif cmd == "target":
                get_target(host, port)
            elif cmd == "status":
                get_status(host, port)
            elif cmd == "logs":
                get_logs(host, port)
            elif cmd in ("help", "?"):
                print("Commands:")
                print("  write <val>   - Set game target (0-100)")
                print("  guess <val>   - Submit a guess (0-100)")
                print("  resolve       - Evaluate current round and pick closest guess")
                print("  target        - Query committed target")
                print("  status        - Check node state (role, leader, epoch, seq)")
                print("  logs          - Display node local transaction log")
                print("  node <1-5>    - Switch connected node")
                print("  exit          - Exit shell")
            else:
                print(f"Unknown command: '{cmd}'. Type 'help' for options.")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting interactive shell.")
            break


def main():
    parser = argparse.ArgumentParser(description="ZooKeeper Game Client (Raw TCP Sockets)")
    parser.add_argument("--node", type=int, default=1, choices=[1, 2, 3, 4, 5], help="Target node ID (1-5)")
    parser.add_argument("--client", type=str, default="Client_A", help="Client identifier name")
    parser.add_argument("--write", type=int, help="Send Write(x) to set game target (0-100)")
    parser.add_argument("--guess", type=int, help="Send Guess(x) to submit guess (0-100)")
    parser.add_argument("--resolve", action="store_true", help="Resolve current round (pick closest guess)")
    parser.add_argument("--target", action="store_true", help="Query current committed target")
    parser.add_argument("--status", action="store_true", help="Query status of connected node")
    parser.add_argument("--logs", action="store_true", help="Query transaction log entries of connected node")
    parser.add_argument("--interactive", action="store_true", help="Launch interactive shell")

    args = parser.parse_args()
    host, port = get_node_address(args.node)

    if args.interactive:
        interactive_mode(args.node, args.client)
    elif args.write is not None:
        send_write(host, port, args.write, args.client)
    elif args.guess is not None:
        send_guess(host, port, args.guess, args.client, resolve_immediately=args.resolve)
    elif args.resolve:
        trigger_resolve_round(host, port)
    elif args.target:
        get_target(host, port)
    elif args.status:
        get_status(host, port)
    elif args.logs:
        get_logs(host, port)
    else:
        # Default action: show status and target
        print(f"Connecting to Node {args.node} at {host}:{port}...")
        get_status(host, port)
        get_target(host, port)


if __name__ == "__main__":
    main()
