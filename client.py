"""ClientApp component for ZooKeeper-like System Lab.

Allows clients to connect to ANY node in the cluster to:
- Send Write(x) to set the game target.
- Send Guess(x) to submit guesses.
- Trigger round evaluation to select closest guess.
- Query current target and cluster status.
- Supports both single-command CLI and interactive shell.
"""

import argparse
import sys
import requests


def send_write(base_url: str, value: int, client_id: str) -> None:
    url = f"{base_url.rstrip('/')}/write"
    try:
        resp = requests.post(url, json={"value": value, "client_id": client_id}, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        print(f"✅ [Write Success]")
        print(f"   Target Set: {value}")
        print(f"   Connected Node: {data.get('handled_by')} ({data.get('role')})")
        if "forwarded_to" in data:
            print(f"   Forwarded to Leader: Node {data.get('forwarded_to')}")
        print(f"   Response Details: {data}")
    except Exception as e:
        print(f"❌ [Write Failed]: {e}")


def send_guess(base_url: str, value: int, client_id: str, resolve_immediately: bool = False) -> None:
    url = f"{base_url.rstrip('/')}/guess"
    try:
        resp = requests.post(
            url,
            json={"value": value, "client_id": client_id, "resolve_immediately": resolve_immediately},
            timeout=5.0,
        )
        resp.raise_for_status()
        data = resp.json()
        print(f"✅ [Guess Submitted]")
        print(f"   Client: {client_id}")
        print(f"   Guess Value: {value}")
        print(f"   Connected Node: {data.get('handled_by')} ({data.get('role')})")
        if "forwarded_to" in data:
            print(f"   Forwarded to Leader: Node {data.get('forwarded_to')}")
        if data.get("resolved"):
            print(f"   Immediate Round Resolution: {data['resolved']}")
    except Exception as e:
        print(f"❌ [Guess Failed]: {e}")


def resolve_round(base_url: str) -> None:
    url = f"{base_url.rstrip('/')}/resolve_round"
    try:
        resp = requests.post(url, timeout=5.0)
        resp.raise_for_status()
        data = resp.json()
        print(f"🏆 [Round Evaluation Result]")
        res = data.get("result", {})
        if res.get("success"):
            print(f"   Winner: {res.get('winner')}")
            print(f"   Winning Guess: {res.get('winning_guess')}")
            print(f"   Old Target: {res.get('old_target')} -> New Target: {res.get('new_target')}")
            print(f"   All Guesses Evaluated: {res.get('all_guesses')}")
            print(f"   Consensus Status: {res.get('consensus', {}).get('status')}")
        else:
            print(f"   Resolution Failed: {res.get('error')}")
    except Exception as e:
        print(f"❌ [Round Evaluation Failed]: {e}")


def get_target(base_url: str) -> None:
    url = f"{base_url.rstrip('/')}/target"
    try:
        resp = requests.get(url, timeout=3.0)
        resp.raise_for_status()
        data = resp.json()
        target = data.get("target")
        print(f"🎯 Current Game Target: {target if target is not None else 'NOT SET YET'}")
        if data.get("target_epoch") is not None:
            print(f"   Committed at: Epoch {data.get('target_epoch')}, Seq {data.get('target_seq')}")
    except Exception as e:
        print(f"❌ [Get Target Failed]: {e}")


def get_status(base_url: str) -> None:
    url = f"{base_url.rstrip('/')}/status"
    try:
        resp = requests.get(url, timeout=3.0)
        resp.raise_for_status()
        data = resp.json()
        print(f"ℹ️ Node {data.get('node_id')} Status:")
        print(f"   Role: {data.get('role')}")
        print(f"   Current Leader: Node {data.get('current_leader')}")
        print(f"   Epoch: {data.get('epoch')}, Seq: {data.get('seq')}")
        print(f"   Target: {data.get('current_target')}")
        print(f"   Total Log Entries: {data.get('log_count')}")
    except Exception as e:
        print(f"❌ [Get Status Failed]: {e}")


def get_log(base_url: str) -> None:
    url = f"{base_url.rstrip('/')}/log"
    try:
        resp = requests.get(url, timeout=3.0)
        resp.raise_for_status()
        entries = resp.json().get("entries", [])
        print(f"📜 Transaction Log (Node {resp.json().get('node_id')}):")
        if not entries:
            print("   (Empty)")
            return
        print(f"   {'Epoch':<7} {'Seq':<6} {'Op':<8} {'Value':<8} {'Status':<12}")
        print("   " + "-" * 45)
        for e in entries:
            print(f"   {e['epoch']:<7} {e['seq']:<6} {e['op']:<8} {e['value']:<8} {e['status']:<12}")
    except Exception as e:
        print(f"❌ [Get Log Failed]: {e}")


def run_interactive(base_url: str, default_client: str) -> None:
    current_url = base_url
    client_id = default_client
    print("\n" + "=" * 60)
    print("🎮 ZooKeeper Game Client Interactive Console")
    print(f"Connected to: {current_url} | Client ID: {client_id}")
    print("Commands: write <val>, guess <val>, resolve, target, status, log, switch <port>, client <name>, quit")
    print("=" * 60 + "\n")

    while True:
        try:
            line = input(f"[{client_id}@{current_url}]> ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nExiting.")
            break

        if not line:
            continue
        parts = line.split()
        cmd = parts[0].lower()

        if cmd in ("quit", "exit"):
            break
        elif cmd == "write" and len(parts) >= 2:
            send_write(current_url, int(parts[1]), client_id)
        elif cmd == "guess" and len(parts) >= 2:
            send_guess(current_url, int(parts[1]), client_id)
        elif cmd == "resolve":
            resolve_round(current_url)
        elif cmd == "target":
            get_target(current_url)
        elif cmd == "status":
            get_status(current_url)
        elif cmd == "log":
            get_log(current_url)
        elif cmd == "switch" and len(parts) >= 2:
            port = parts[1]
            if not port.startswith("http"):
                current_url = f"http://127.0.0.1:{port}"
            else:
                current_url = port
            print(f"Switched connection to {current_url}")
        elif cmd == "client" and len(parts) >= 2:
            client_id = parts[1]
            print(f"Client ID changed to {client_id}")
        elif cmd == "help":
            print("Available commands:")
            print("  write <0-100>          : Set new game target")
            print("  guess <0-100>          : Submit guess for current round")
            print("  resolve                : Evaluate round and commit closest guess")
            print("  target                 : Print current game target")
            print("  status                 : Print node status")
            print("  log                    : View transaction log")
            print("  switch <port>          : Connect to a different node (e.g. switch 5002)")
            print("  client <name>          : Change client identifier")
            print("  quit                   : Exit shell")
        else:
            print("Unknown command. Type 'help' for available commands.")


def main():
    parser = argparse.ArgumentParser(description="ZooKeeper Game Client")
    parser.add_argument("--node", default="http://127.0.0.1:5001", help="Target node URL (default: http://127.0.0.1:5001)")
    parser.add_argument("--client", default="Client_A", help="Client identifier (default: Client_A)")

    subparsers = parser.add_subparsers(dest="command")

    # write command
    p_write = subparsers.add_parser("write", help="Set target value (0-100)")
    p_write.add_argument("value", type=int, help="Target integer between 0 and 100")

    # guess command
    p_guess = subparsers.add_parser("guess", help="Submit a guess")
    p_guess.add_argument("value", type=int, help="Guess integer between 0 and 100")
    p_guess.add_argument("--resolve", action="store_true", help="Resolve round immediately after guess")

    # resolve command
    subparsers.add_parser("resolve", help="Evaluate current round and select closest guess")

    # target command
    subparsers.add_parser("target", help="Query current committed target")

    # status command
    subparsers.add_parser("status", help="Query node status and role")

    # log command
    subparsers.add_parser("log", help="Display node local transaction log")

    # interactive mode
    subparsers.add_parser("interactive", help="Start interactive prompt")

    args = parser.parse_args()

    if args.command == "write":
        send_write(args.node, args.value, args.client)
    elif args.command == "guess":
        send_guess(args.node, args.value, args.client, resolve_immediately=args.resolve)
    elif args.command == "resolve":
        resolve_round(args.node)
    elif args.command == "target":
        get_target(args.node)
    elif args.command == "status":
        get_status(args.node)
    elif args.command == "log":
        get_log(args.node)
    elif args.command == "interactive":
        run_interactive(args.node, args.client)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
