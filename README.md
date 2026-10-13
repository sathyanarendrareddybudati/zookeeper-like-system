# ZooKeeper-like Consensus System & Game Application
**Course:** II.3502 - Distributed Architecture and Programming  
**Implementation:** Python 3, FastAPI, SQLite, Asyncio

---

## 📌 Overview

This project implements a simplified, high-performance **ZooKeeper-like distributed coordination system** based on the **ZAB (ZooKeeper Atomic Broadcast)** consensus protocol. On top of this distributed coordination foundation, a turn-based number guessing game is implemented to validate system behavior under realistic multi-client interactions.

### Key Features
- **Topology:** 5 nodes running locally on ports `5001`–`5005`.
- **Quorum Consensus:** Quorum threshold is **3** ($\lfloor 5/2 \rfloor + 1$). Updates are committed only after acknowledgment from a majority.
- **Node Roles:** Dynamic **Leader** and **Follower** roles with automatic role transitions.
- **Request Forwarding:** Clients can connect to **any node**; followers transparently forward writes and guesses to the active leader.
- **Leader Failure & Re-Election:** Heartbeat monitoring detects leader crashes. Nodes elect a new leader based on highest `(epoch, seq)` with ties broken by lowest node ID.
- **Log Recovery & Synchronization:** New leader reconciles uncommitted entries and syncs logs before incrementing the epoch and resuming operations. Rejoined nodes catch up automatically.
- **Persistent Storage:** Local SQLite database per node (`data/node_<id>.db`) tracking `(epoch, seq, op, value, status)`.
- **Live Visual Dashboard:** Built-in web UI on all nodes (`/` and `/dashboard`) with real-time cluster monitoring and interactive controls.
- **OpenAPI / Swagger Documentation:** Interactive API documentation available at `http://localhost:<port>/docs`.

---

## 🚀 Quick Start

### 1. Requirements
Ensure Python 3.10+ and the required packages are installed:
```bash
pip install fastapi uvicorn httpx requests pydantic
```

---

### 2. Running the Complete 5-Node Cluster
To start all 5 nodes concurrently with a single command:
```bash
python run_cluster.py --clean
```
*The `--clean` flag wipes previous SQLite databases in `data/` for a fresh start.*

You will see the cluster startup logs:
- **Node 1 (Initial Leader):** `http://127.0.0.1:5001`
- **Node 2 (Follower):** `http://127.0.0.1:5002`
- **Node 3 (Follower):** `http://127.0.0.1:5003`
- **Node 4 (Follower):** `http://127.0.0.1:5004`
- **Node 5 (Follower):** `http://127.0.0.1:5005`

To terminate all nodes cleanly, press `Ctrl+C`.

---

### 3. Running an Individual Node
You can also launch nodes individually in separate terminal tabs:
```bash
python node_server.py --id 1
python node_server.py --id 2
python node_server.py --id 3
python node_server.py --id 4
python node_server.py --id 5
```

---

## 🧪 Automated Testing of the 3 Lab Scenarios

The lab requires demonstration of three core scenarios:
1. **Normal write flow**
2. **Guessing flow**
3. **Leader crash and recovery**

An automated test script is provided that executes and validates all three scenarios:
```bash
python test_scenarios.py
```

### Expected Output
```text
======================================================================
▶️ TEST SCENARIO 1: Normal Write Flow
   Target: 50 | Client connects to Follower Node 3 (tests forwarding)
======================================================================
1. Client sending POST /write (value=50) to Follower Node 3...
   Response: {'handled_by': 3, 'forwarded_to': 1, 'role': 'FOLLOWER', ...}
2. Verifying committed target across all 5 nodes...
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
2. Client_Bob submits Guess(62) to Node 4...
3. Client triggers round evaluation via Node 5...
   Evaluation response: {'winner': 'Client_Bob', 'winning_guess': 62, 'new_target': 62, ...}
4. Verifying new target (62) across all 5 nodes...
✅ SCENARIO 2 PASSED: Closest guess picked, proposed, committed by quorum, new round began!

======================================================================
▶️ TEST SCENARIO 3: Leader Crash and Recovery
   Current Leader: Node 1
   Action: Terminate Node 1 -> Followers detect heartbeat loss -> Elect new leader
======================================================================
💥 Killing Node 1 process...
1. Waiting for followers to detect timeout and elect new leader...
   🎉 Detected new leader: Node 3 (Epoch: 2, Seq: 0)
2. Verifying cluster consensus under new Leader Node 3...
3. Resuming operations: Client sends Write(85) to Node 3 under new leader...
4. Verifying target 85 across surviving nodes...
5. Restarting Node 1 to verify rejoin and state catchup...
   Node 1 rejoined status: Role=FOLLOWER, Leader=Node 3, Target=85
======================================================================
🎉 ALL THREE TESTING SCENARIOS PASSED WITH 100% SUCCESS! 🎉
======================================================================
```

---

## 🎮 Using the Client Application (`client.py`)

Clients can interact with the cluster either via command-line arguments or via an interactive prompt.

### Single-Command Mode
Connect to **any node** (leader or follower):
```bash
# Set target (Write 50 to follower Node 2)
python client.py --node http://127.0.0.1:5002 write 50

# Submit guesses from different clients to different nodes
python client.py --node http://127.0.0.1:5003 --client Alice guess 30
python client.py --node http://127.0.0.1:5004 --client Bob guess 62

# Evaluate round (select closest guess and commit as new target)
python client.py --node http://127.0.0.1:5005 resolve

# Query current target from any node
python client.py --node http://127.0.0.1:5001 target

# Inspect node status and cluster role
python client.py --node http://127.0.0.1:5003 status

# View local transaction log
python client.py --node http://127.0.0.1:5001 log
```

### Interactive Console Mode
```bash
python client.py --node http://127.0.0.1:5001 interactive
```
Commands available inside the interactive console:
- `write <value>`: Set target (0–100)
- `guess <value>`: Submit a guess
- `resolve`: Trigger closest guess evaluation
- `target`: View current committed target
- `status`: Check current node role, leader, epoch
- `log`: Display persistent transaction log
- `switch <port>`: Switch connection to another node (e.g. `switch 5003`)
- `client <name>`: Switch client username
- `quit`: Exit shell

---

## 🌐 Web Dashboard

Open your browser to:
👉 **`http://127.0.0.1:5001/`** (or port 5002–5005)

The dashboard provides:
- Live status badges for all 5 nodes (Leader, Follower, Offline).
- Real-time display of current target, epoch, sequence number, and active leader.
- Interactive buttons to submit **Write**, **Guess**, and **Resolve Round**.
- Live view of the persistent transaction log on the active node.

---

## 📁 Repository Structure

```
.
├── config.py             # Cluster topology (ports 5001-5005), quorum (3), timing parameters
├── log_store.py          # SQLite persistent log store (epoch, seq, op, value, status)
├── leader_coordinator.py # Leader proposal broadcast, ACK gathering, quorum check, commit broadcast
├── election_manager.py   # Heartbeat monitoring, ZAB election logic, log sync, epoch increment
├── node_server.py        # FastAPI server with peer RPC endpoints, client handlers, and web dashboard
├── client.py             # ClientApp CLI and interactive shell
├── run_cluster.py        # Process manager for launching and terminating the 5-node cluster
├── test_scenarios.py     # Automated test suite for all 3 scenarios
├── README.md             # Run instructions and usage guide
├── REPORT.md             # Academic lab report and oral defence analysis
└── lab_zookeeper.pdf     # Original lab specification
```
