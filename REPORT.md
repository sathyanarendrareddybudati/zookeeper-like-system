# Lab Report: Fault Tolerance, Leader Election & ZooKeeper-like System
**Course:** II.3502 - Distributed Architecture and Programming (2026/27)  
**Deliverable:** System Implementation, Analysis, and Test Report  

---

## 1. Executive Summary & System Objectives

The goal of this project is to implement a fault-tolerant, ZooKeeper-like distributed coordination system based on the **ZooKeeper Atomic Broadcast (ZAB)** consensus protocol, coupled with a distributed turn-based number guessing game.

The system meets all core objectives specified in the lab assignment:
1. **Consensus & Log Replication:** Implements the ZAB `PROPOSAL -> ACK -> COMMIT` sequence driven by an elected leader.
2. **Quorum Guarantees:** Enforces quorum commitment ($\lfloor N/2 \rfloor + 1 = 3$ for $N=5$ nodes), guaranteeing safety under crash faults.
3. **Leader Failure & Re-election:** Automated heartbeat monitoring detects leader termination; survivors elect a new leader using the highest `(epoch, seq)` rule with ties broken by lowest node ID.
4. **Log Reconciliation & Epoch Bumping:** The newly elected leader reconciles uncommitted proposals across quorum logs, increments the epoch counter, and resets the sequence number to 0.
5. **Transparent Client Forwarding:** Clients may connect to any node; followers forward write/guess requests to the leader without prematurely logging them.

---

## 2. Architecture & Design Choices

```
       +---------------------------------------------+
       |           Client Application (CLI/Web)      |
       +---------------------------------------------+
               | (Connects to ANY node)
               v
       +-----------------------+
       |   Follower (Node 3)   |
       +-----------------------+
               | FORWARD
               v
       +-----------------------+
       |   Leader (Node 1)     | <--- Heartbeats & Proposals
       +-----------------------+
        /      |          \
       v       v           v
  Follower 2 Follower 4 Follower 5
```

### 2.1 Communication Paradigm: FastAPI (HTTP + JSON)
* **Choice:** FastAPI with asynchronous `httpx` peer communication and structured JSON message schemas.
* **Rationale:** Compared to raw TCP sockets, FastAPI eliminates fragile custom byte-stream framing and delimiter bugs while providing automatic schema validation via Pydantic, non-blocking asynchronous event loops, built-in Swagger/OpenAPI documentation (`/docs`), and an interactive web dashboard (`/dashboard`) for oral defense demonstrations.

### 2.2 Concurrency & Asynchronous Quorum Collection
* **Choice:** Python `asyncio` with `asyncio.wait` and explicit timeouts (`ACK_TIMEOUT = 2.5s`).
* **Rationale:** Asynchronous dispatch ensures that unresponsive or crashed nodes do not block the leader indefinitely. The leader collects acknowledgments concurrently and commits as soon as the quorum threshold is reached.

### 2.3 Persistent Log Storage: SQLite
* **Choice:** SQLite database per node (`data/node_<id>.db`).
* **Schema:**
  ```sql
  CREATE TABLE logs (
      epoch   INTEGER NOT NULL,
      seq     INTEGER NOT NULL,
      op      TEXT NOT NULL,
      value   INTEGER NOT NULL,
      status  TEXT NOT NULL, -- 'PROPOSED' or 'COMMITTED'
      PRIMARY KEY (epoch, seq)
  );
  ```
* **Rationale:** SQLite provides full ACID durability. If a node crashes and recovers, its history is safely stored on disk and can be retrieved immediately.

---

## 3. Protocol Mechanics

### 3.1 ZAB Consensus Cycle
When a `Write(x)` or winning `Guess(x)` is processed:
1. **Tag Assignment:** The leader assigns `epoch` and increments `seq` (`seq += 1`).
2. **Local Proposal:** Leader writes `(epoch, seq, op, value, 'PROPOSED')` to its SQLite log.
3. **Broadcast:** Leader sends `POST /propose` to all followers.
4. **Follower Logging:** Followers write the entry as `'PROPOSED'` and reply with `ACK`.
5. **Quorum Check:** Leader counts ACKs (leader self-ACK + follower ACKs). Upon receiving $\ge 3$ ACKs, leader updates status to `'COMMITTED'`.
6. **Commit Broadcast:** Leader sends `POST /commit` to all followers.
7. **State Machine Update:** Followers commit the entry in their SQLite log and update the publicly visible target.

```
Leader                      Followers (F1, F2, F3, F4)
  |                                     |
  |--- PROPOSE(epoch, seq, op, val) --->|
  |<------------- ACK ------------------| (from >= 2 followers)
  | [Quorum Reached: Leader Commits]    |
  |--- COMMIT(epoch, seq) ------------->|
  |                                     | [Followers Commit]
```

### 3.2 Heartbeat & Leader Election
* **Heartbeat:** The leader transmits `POST /heartbeat` every `1.0s`.
* **Timeout Detection:** Followers track time since last heartbeat. If `elapsed > timeout` (randomized between `2.5s` and `4.0s` to prevent split votes), follower transitions to `CANDIDATE`.
* **Election Rule:**
  - Candidate broadcasts `ELECTION(cand_epoch, cand_seq, cand_id)`.
  - A peer votes **YES** if:
    $$(cand\_epoch, cand\_seq, -cand\_id) \ge (my\_epoch, my\_seq, -my\_id)$$
    *Ties are broken by lowest node ID.*
* **Winning Quorum:** Upon gathering $\ge 3$ votes:
  1. The new leader initiates **Log Synchronisation**: queries follower logs and reconciles uncommitted entries.
  2. The new leader increments the epoch (`epoch = cand_epoch + 1`) and resets sequence (`seq = 0`).
  3. The new leader resumes coordinator duties and begins broadcasting heartbeats.
* **Follower Rejoin Catchup:** When a crashed node restarts, its first incoming heartbeat with a higher epoch triggers an automatic log synchronization with the active leader.

---

## 4. Game Application Logic

* **Target Setup:** Client sends `Write(x)` ($0 \le x \le 100$). Once committed by quorum, this becomes the baseline target.
* **Guess Submission:** Clients submit `Guess(x)` through any node. Followers forward guesses to the leader's round pool.
* **Round Resolution:**
  - The leader calculates $\Delta = |guess - target|$ for all guesses in the current round.
  - The closest guess is selected (ties broken by earliest arrival).
  - The leader proposes the winning value with `op="GUESS"` through the full consensus pipeline (`PROPOSE -> ACK -> COMMIT`).
  - Upon commit, the winning guess becomes the new target and the next round begins.

---

## 5. Verification of the 3 Testing Scenarios

All tests were executed against the live 5-node cluster via `test_scenarios.py`.

### Scenario 1: Normal Write Flow
* **Test Case:** Client connects to Follower Node 3 and sends `Write(50)`.
* **Observed Execution:**
  ```text
  1. Client sending POST /write (value=50) to Follower Node 3 (http://127.0.0.1:5003)...
     Response: {'handled_by': 3, 'forwarded_to': 1, 'role': 'FOLLOWER',
                'response': {'forwarded': True, 'result': {'success': True, 'epoch': 1, 'seq': 1,
                             'op': 'WRITE', 'value': 50, 'status': 'COMMITTED', 'ack_count': 5}}}
  2. Verifying committed target across all 5 nodes...
     Node 1: Target = 50 (Epoch: 1, Seq: 1)
     Node 2: Target = 50 (Epoch: 1, Seq: 1)
     Node 3: Target = 50 (Epoch: 1, Seq: 1)
     Node 4: Target = 50 (Epoch: 1, Seq: 1)
     Node 5: Target = 50 (Epoch: 1, Seq: 1)
  ✅ SCENARIO 1 PASSED: Target successfully proposed, ACKed by quorum, committed, and visible across all nodes!
  ```
* **Analysis:** Follower Node 3 correctly forwarded the client write to Leader Node 1 without logging it first. Leader Node 1 assigned `(epoch=1, seq=1)`, collected 5 ACKs, committed the value, broadcasted commit, and all 5 nodes updated their target to 50.

---

### Scenario 2: Guessing Flow
* **Test Case:** Baseline target is 50. Client_Alice submits 30 to Node 2; Client_Bob submits 62 to Node 4. Client requests round resolution via Node 5.
* **Observed Execution:**
  ```text
  1. Client_Alice submits Guess(30) to Node 2...
     Node 2 response: {'handled_by': 2, 'forwarded_to': 1, 'role': 'FOLLOWER', ...}
  2. Client_Bob submits Guess(62) to Node 4...
     Node 4 response: {'handled_by': 4, 'forwarded_to': 1, 'role': 'FOLLOWER', ...}
  3. Client triggers round evaluation via Node 5...
     Evaluation response: {'handled_by': 5, 'forwarded_to': 1, 'role': 'FOLLOWER',
     'response': {'handled_by': 1, 'role': 'LEADER',
                  'result': {'success': True, 'winner': 'Client_Bob', 'winning_guess': 62,
                             'old_target': 50, 'new_target': 62, 'difference': 12,
                             'all_guesses': [{'client_id': 'Client_Alice', 'value': 30},
                                             {'client_id': 'Client_Bob', 'value': 62}],
                             'consensus': {'success': True, 'epoch': 1, 'seq': 2, 'op': 'GUESS',
                                           'value': 62, 'status': 'COMMITTED', 'ack_count': 5}}}}
  4. Verifying new target (62) across all 5 nodes...
     Node 1: Target = 62
     Node 2: Target = 62
     Node 3: Target = 62
     Node 4: Target = 62
     Node 5: Target = 62
  ✅ SCENARIO 2 PASSED: Closest guess picked, proposed, committed by quorum, new round began!
  ```
* **Analysis:** Since $|62 - 50| = 12 < |30 - 50| = 20$, Client_Bob's guess (62) was identified as the winner. The leader executed the consensus cycle for `op="GUESS"` (`epoch=1, seq=2`), committed 62, and all nodes reflected the new target.

---

### Scenario 3: Leader Crash and Recovery
* **Test Case:** Current Leader Node 1 is abruptly terminated. Surviving followers (2, 3, 4, 5) detect missing heartbeats, elect a new leader, synchronize logs, and accept subsequent writes. Node 1 is then restarted to verify rejoin.
* **Observed Execution:**
  ```text
  💥 Killing Node 1 process (PID: 4750)...
  1. Waiting for followers to detect timeout and elect new leader...
     🎉 Detected new leader: Node 3 (Epoch: 2, Seq: 0)
  2. Verifying cluster consensus under new Leader Node 3...
     Node 2: Role=FOLLOWER, Leader=Node 3, Epoch=2
     Node 3: Role=LEADER, Leader=Node 3, Epoch=2
     Node 4: Role=FOLLOWER, Leader=Node 3, Epoch=2
     Node 5: Role=FOLLOWER, Leader=Node 3, Epoch=2
  3. Resuming operations: Client sends Write(85) to Node 3 under new leader...
     Write response: {'handled_by': 3, 'role': 'LEADER', 'client_id': 'Client_PostRecovery', 'value': 85,
                      'result': {'success': True, 'epoch': 2, 'seq': 1, 'op': 'WRITE', 'value': 85,
                                 'status': 'COMMITTED', 'ack_count': 4}}
  4. Verifying target 85 across surviving nodes...
     Node 2: Target = 85
     Node 3: Target = 85
     Node 4: Target = 85
     Node 5: Target = 85
  5. Restarting Node 1 to verify rejoin and state catchup...
     Node 1 rejoined status: Role=FOLLOWER, Leader=Node 3, Target=85
  ✅ SCENARIO 3 PASSED: Leader failure detected, Node 3 elected leader, epoch bumped to 2, target 85 committed, restarted Node 1 successfully caught up!
  ```
* **Analysis:**
  - Node 1 termination stopped heartbeats.
  - Survivors timed out after ~3.0s and started election.
  - Candidate Node 3 received quorum votes ($\ge 3$), became leader, synchronized logs, incremented epoch to 2, and reset seq to 0.
  - A new write `Write(85)` was committed by the surviving quorum.
  - When Node 1 restarted, it recognized Leader Node 3, adopted epoch 2, and synchronized its missing log entries to reach Target 85.

---

## 6. Oral Defence Preparation Cheat Sheet

| Question | Core Concept to Explain |
| :--- | :--- |
| **Why is Quorum set to 3?** | For $N=5$, any two majorities of size 3 must intersect in at least 1 node ($3 + 3 = 6 > 5$). This overlap guarantees that at least one surviving node has the latest committed transaction, preventing split-brain. |
| **How does your election prevent split-brain?** | Candidates must collect votes from a strict majority (at least 3 out of 5). Since two separate majorities cannot exist simultaneously, at most one leader can be elected per epoch. |
| **What happens to uncommitted proposals when the leader crashes?** | During the synchronization phase, the new leader collects logs from reachable peers. If an entry reached a quorum of proposed copies, it is committed; if it failed to reach quorum, it is discarded or overwritten, preserving linearizability. |
| **Why increment epoch and reset sequence number to 0?** | The epoch distinguishes leader tenures. Bumping epoch prevents an old, partitioned leader from making proposals with overlapping sequence numbers. Sequence resets to 0 so proposals in each epoch are monotonically sequenced from $(epoch, 1)$. |
| **Why does a follower NOT log upon receiving a write?** | Per ZAB specification, followers only log operations ordered and tagged by the leader. Followers forward client writes to the leader and log only when the official `PROPOSE(epoch, seq)` arrives. |

---

## 7. Conclusion

The developed solution fulfills all functional and architectural requirements of the ZooKeeper lab. By combining **FastAPI**, **SQLite**, and **asyncio**, the system achieves robust consensus, transparent request forwarding, reliable crash recovery, and live visibility suitable for presentation and academic evaluation.
