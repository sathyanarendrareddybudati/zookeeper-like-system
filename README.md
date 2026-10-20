# ZooKeeper-like Consensus System & Game Application
**Course:** II.3502 - Distributed Architecture and Programming  
**Implementation Technology:** Python 3 Standard Library (Raw TCP Sockets, Multi-Threading, SQLite) — Zero External Dependencies!

---

## 📌 Overview

This project implements a simplified, robust **ZooKeeper-like distributed coordination system** based on the **ZAB (ZooKeeper Atomic Broadcast)** consensus protocol. Conforming to the lab prerequisites (Labs 1–2 concepts: sockets, threads/executors, basic I/O, and consensus/quorum), all inter-node communication and client-to-node interactions use **raw TCP sockets** with a **line-delimited JSON message protocol** (`\n` framing).

On top of this distributed consensus foundation, a turn-based number guessing game is implemented to validate system behavior under realistic multi-client interactions.

### Key Architecture & Protocol Highlights
- **Communication Paradigm:** Raw TCP sockets (`socket` module) with line-delimited JSON framing.
- **Concurrency & I/O:** Multi-threaded architecture utilizing `threading` and `concurrent.futures.ThreadPoolExecutor` to handle concurrent connections, heartbeat monitoring, and parallel quorum voting.
- **Topology:** 5 nodes running locally on ports `5001`–`5005`.
- **Quorum Consensus:** Quorum threshold is **3** ($\lfloor 5/2 \rfloor + 1$). Updates are committed only after acknowledgment from a majority.
- **Node Roles:** Dynamic **Leader**, **Follower**, and **Candidate** roles with automatic role transitions.
- **Transparent Request Forwarding:** Clients can connect to **any node**; followers transparently forward writes, guesses, and round resolutions to the active leader via `FORWARD` messages.
- **Leader Failure & Re-Election (ZAB Algorithm):** Heartbeat monitoring detects leader crashes. Nodes elect a new leader based on highest `(epoch, seq)` with ties broken by lowest node ID.
- **Log Recovery & Synchronization:** New leader reconciles uncommitted entries and syncs logs before incrementing the epoch and resuming operations. Rejoined nodes catch up automatically.
- **Persistent Storage:** Local SQLite database per node (`data/node_<id>.db`) tracking `(epoch, seq, op, value, status)`.

---

## 📬 Lab Message Protocol Specification

The system implements the exact message types specified in the lab manual (Page 6):

| Message Type | Fields | Description |
| :--- | :--- | :--- |
| `PROPOSE` | `epoch`, `seq`, `op`, `value`, `from` | Sent by Leader to Followers to propose a transaction. |
| `ACK` | `epoch`, `seq`, `from`, `ack` | Sent by Followers to Leader acknowledging log append. |
| `COMMIT` | `epoch`, `seq`, `from` | Broadcast by Leader to Followers upon reaching quorum. |
| `HEARTBEAT` | `epoch`, `from` | Periodic liveness heartbeat sent by Leader to Followers. |
| `ELECTION` | `epoch`, `seq`, `from` | Sent by Candidates to request votes during leader election. |
| `FORWARD` | `op`, `value`, `from_client` | Sent by Followers to forward client Write/Guess requests to Leader. |

---

## 🚀 Quick Start

### 1. Requirements
Only **Python 3.10+** standard library is required! Zero external pip dependencies needed (`fastapi`, `uvicorn`, `httpx`, `pydantic` have been removed in favor of pure sockets).

---

### 2. Running the Complete 5-Node Cluster
To start all 5 nodes concurrently with a single command:
```bash
python3 run_cluster.py --clean
```
*The `--clean` flag wipes previous SQLite databases in `data/` for a clean slate.*

Cluster ports:
- **Node 1 (Initial Leader):** `127.0.0.1:5001`
- **Node 2 (Follower):** `127.0.0.1:5002`
- **Node 3 (Follower):** `127.0.0.1:5003`
- **Node 4 (Follower):** `127.0.0.1:5004`
- **Node 5 (Follower):** `127.0.0.1:5005`

To terminate all nodes cleanly, press `Ctrl+C`.

---

### 3. Running an Individual Node
You can also launch nodes individually in separate terminal windows:
```bash
python3 node_server.py --id 1
python3 node_server.py --id 2
python3 node_server.py --id 3
python3 node_server.py --id 4
python3 node_server.py --id 5
```

---

## 🧪 Automated Testing of the 3 Lab Scenarios

The lab requires demonstration of three core scenarios:
1. **Normal write flow** (Client sets target $\rightarrow$ leader proposes $\rightarrow$ quorum ACKs $\rightarrow$ COMMIT $\rightarrow$ all clients see target).
2. **Guessing flow** (Multiple clients submit guesses $\rightarrow$ leader picks closest $\rightarrow$ commits new target $\rightarrow$ next round begins).
3. **Leader crash and recovery** (Kill leader $\rightarrow$ followers detect missing heartbeat $\rightarrow$ new leader elected $\rightarrow$ logs synchronised $\rightarrow$ operations resume).

Run the automated validation suite:
```bash
python3 test_scenarios.py
```

### Verified Test Output
```text
======================================================================
▶️ TEST SCENARIO 1: Normal Write Flow
   Target: 50 | Client connects to Follower Node 3 (tests forwarding)
======================================================================
1. Client sending WRITE (value=50) to Follower Node 3 (127.0.0.1:5003)...
   Response: {'status': 'ok', 'role': 'LEADER', 'handled_by': 1, 'result': {'success': True, 'epoch': 1, 'seq': 1, 'op': 'WRITE', 'value': 50, 'status': 'COMMITTED', 'ack_count': 5}, 'forwarded_by': 3, 'forwarded_to': 1, 'connected_node': 3, 'connected_role': 'FOLLOWER'}
2. Verifying committed target across all 5 nodes via TCP sockets...
   Node 1: Target = 50 (Epoch: 1, Seq: 1)
   Node 2: Target = 50 (Epoch: 1, Seq: 1)
   Node 3: Target = 50 (Epoch: 1, Seq: 1)
   Node 4: Target = 50 (Epoch: 1, Seq: 1)
   Node 5: Target = 50 (Epoch: 1, Seq: 1)
✅ SCENARIO 1 PASSED: Target successfully proposed, ACKed by quorum, committed, and visible across all nodes!

======================================================================
▶️ TEST SCENARIO 2: Guessing Flow
   Current Target: 50
   Clients submit guesses: Client_Alice -> 30, Client_Bob -> 62
   Expected Winner: Client_Bob (62 is closer to 50 than 30)
======================================================================
1. Client_Alice submits Guess(30) to Node 2...
   Node 2 response: {'status': 'ok', 'role': 'LEADER', 'handled_by': 1, 'guess_result': {'recorded': True, 'guess': {'client_id': 'Client_Alice', 'value': 30}, 'total_guesses_in_round': 1}, 'forwarded_by': 2, 'forwarded_to': 1, 'connected_node': 2, 'connected_role': 'FOLLOWER'}
2. Client_Bob submits Guess(62) to Node 4...
   Node 4 response: {'status': 'ok', 'role': 'LEADER', 'handled_by': 1, 'guess_result': {'recorded': True, 'guess': {'client_id': 'Client_Bob', 'value': 62}, 'total_guesses_in_round': 2}, 'forwarded_by': 4, 'forwarded_to': 1, 'connected_node': 4, 'connected_role': 'FOLLOWER'}
3. Client triggers round evaluation via Node 5...
   Evaluation response: {'status': 'ok', 'role': 'LEADER', 'handled_by': 1, 'result': {'success': True, 'winner': 'Client_Bob', 'winning_guess': 62, 'old_target': 50, 'new_target': 62, 'difference': 12, 'all_guesses': [{'client_id': 'Client_Alice', 'value': 30}, {'client_id': 'Client_Bob', 'value': 62}], 'consensus': {'success': True, 'epoch': 1, 'seq': 2, 'op': 'GUESS', 'value': 62, 'status': 'COMMITTED', 'ack_count': 5}}, 'forwarded_by': 5, 'forwarded_to': 1, 'connected_node': 5, 'connected_role': 'FOLLOWER'}
4. Verifying new target (62) across all 5 nodes...
   Node 1: Target = 62
   Node 2: Target = 62
   Node 3: Target = 62
   Node 4: Target = 62
   Node 5: Target = 62
✅ SCENARIO 2 PASSED: Closest guess picked, proposed, committed by quorum, new round began!

======================================================================
▶️ TEST SCENARIO 3: Leader Crash and Recovery
   Current Leader: Node 1
   Action: Terminate Node 1 -> Followers detect heartbeat loss -> Elect new leader
======================================================================
💥 Killing Node 1 process (PID: 79767)...
1. Waiting for followers to detect timeout and elect new leader...
   🎉 Detected new leader: Node 2 (Epoch: 2, Seq: 2)
2. Verifying cluster consensus under new Leader Node 2...
   Node 2: Role=LEADER, Leader=Node 2, Epoch=2
   Node 3: Role=FOLLOWER, Leader=Node 2, Epoch=2
   Node 4: Role=FOLLOWER, Leader=Node 2, Epoch=2
   Node 5: Role=FOLLOWER, Leader=Node 2, Epoch=2
3. Resuming operations: Client sends Write(85) to Node 3 under new leader...
   Write response: {'status': 'ok', 'role': 'LEADER', 'handled_by': 2, 'result': {'success': True, 'epoch': 2, 'seq': 1, 'op': 'WRITE', 'value': 85, 'status': 'COMMITTED', 'ack_count': 4}, 'forwarded_by': 3, 'forwarded_to': 2, 'connected_node': 3, 'connected_role': 'FOLLOWER'}
4. Verifying target 85 across surviving nodes...
   Node 2: Target = 85
   Node 3: Target = 85
   Node 4: Target = 85
   Node 5: Target = 85
5. Restarting Node 1 to verify rejoin and state catchup...
   Node 1 rejoined status: Role=FOLLOWER, Leader=Node 2, Target=85

======================================================================
🎉 ALL THREE TESTING SCENARIOS PASSED WITH 100% SUCCESS! 🎉
======================================================================
```

---

## 🎮 Interactive Client Application (`client.py`)

Clients can connect to **any node** over TCP sockets to play the game, inspect state, and trigger rounds.

### Command-Line Usage
```bash
# Set game target (connect to Follower Node 3)
python3 client.py --node 3 --write 50 --client Alice

# Submit guesses from different clients to different nodes
python3 client.py --node 2 --guess 30 --client Alice
python3 client.py --node 4 --guess 62 --client Bob

# Trigger round resolution (closest guess wins)
python3 client.py --node 5 --resolve

# Query current committed target
python3 client.py --node 1 --target

# Inspect node role, epoch, seq, and leader
python3 client.py --node 2 --status

# View local persistent SQLite logs
python3 client.py --node 2 --logs
```

### Interactive Shell Mode
```bash
python3 client.py --interactive --client Alice --node 2
```
In the interactive shell, you can run:
- `write 50`
- `guess 42`
- `resolve`
- `target`
- `status`
- `logs`
- `node 4` (switch connected node)
- `exit`

---

## 🏛️ Component Architecture

1. [NodeServer](file:///Users/satyanarendrareddybudati/Desktop/lab_zookeeper/node_server.py): Listens on raw TCP socket, accepts connections, and dispatches JSON messages (`PROPOSE`, `ACK`, `COMMIT`, `HEARTBEAT`, `ELECTION`, `FORWARD`, etc.) concurrently using a `ThreadPoolExecutor`.
2. [LeaderCoordinator](file:///Users/satyanarendrareddybudati/Desktop/lab_zookeeper/leader_coordinator.py): Drives the ZAB 2-phase commit cycle (`PROPOSE` $\rightarrow$ quorum `ACK` $\rightarrow$ `COMMIT`) and manages round guesses and closest-value evaluation.
3. [ElectionManager](file:///Users/satyanarendrareddybudati/Desktop/lab_zookeeper/election_manager.py): Background thread monitoring leader heartbeats, triggers randomized timeout election, handles votes, synchronizes logs across followers upon winning, increments epoch, and resets seq to 0.
4. [LogStore](file:///Users/satyanarendrareddybudati/Desktop/lab_zookeeper/log_store.py): Thread-safe SQLite store persisting `(epoch, seq, op, value, status)` locally on each node.
5. [ClientApp](file:///Users/satyanarendrareddybudati/Desktop/lab_zookeeper/client.py): Socket client for interacting with the cluster.
6. [network_utils](file:///Users/satyanarendrareddybudati/Desktop/lab_zookeeper/network_utils.py): Socket helpers with line-delimited JSON framing (`send_rpc`).
