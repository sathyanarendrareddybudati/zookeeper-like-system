"""NodeServer implementation using FastAPI.

Implements the ZooKeeper-like NodeServer interface:
- Accepts peer connections for PROPOSE, ACK, COMMIT, HEARTBEAT, ELECTION, FORWARD.
- Accepts client requests for Write(x), Guess(x), Target, Status, Log.
- Forwards client requests to the leader if this node is a follower.
- Serves an interactive visual dashboard for live inspection and defence demonstration.
"""

import argparse
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, List

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from config import NODES, REQUEST_TIMEOUT
from log_store import LogStore
from leader_coordinator import LeaderCoordinator
from election_manager import ElectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("NodeServer")


# --- Pydantic Message Schemas ---

class ProposeMessage(BaseModel):
    epoch: int
    seq: int
    op: str
    value: int
    from_node: int

class AckMessage(BaseModel):
    epoch: int
    seq: int
    from_node: int

class CommitMessage(BaseModel):
    epoch: int
    seq: int
    from_node: Optional[int] = None

class HeartbeatMessage(BaseModel):
    epoch: int
    from_node: int

class ElectionMessage(BaseModel):
    epoch: int
    seq: int
    from_node: int

class ForwardMessage(BaseModel):
    op: str
    value: int
    client_id: Optional[str] = "client_unknown"

class WriteRequest(BaseModel):
    value: int = Field(..., ge=0, le=100, description="Target value between 0 and 100")
    client_id: Optional[str] = "Client_Default"

class GuessRequest(BaseModel):
    value: int = Field(..., ge=0, le=100, description="Guess value between 0 and 100")
    client_id: Optional[str] = "Client_Default"
    resolve_immediately: Optional[bool] = False

class SyncLogsRequest(BaseModel):
    entries: List[Dict[str, Any]]


# --- Node Application Factory ---

def create_app(node_id: int) -> FastAPI:
    log_store = LogStore(node_id=node_id)
    coordinator = LeaderCoordinator(node_id=node_id, log_store=log_store)
    election_manager = ElectionManager(node_id=node_id, log_store=log_store, coordinator=coordinator)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Startup
        logger.info(f"[Node {node_id}] Initializing NodeServer on port {NODES[node_id]['port']}...")
        election_manager.start()
        yield
        # Shutdown
        logger.info(f"[Node {node_id}] Shutting down NodeServer...")
        election_manager.stop()

    app = FastAPI(
        title=f"ZooKeeper Node {node_id}",
        description="ZooKeeper-like Consensus System with ZAB Protocol and Game Application",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Attach instances to app state
    app.state.node_id = node_id
    app.state.log_store = log_store
    app.state.coordinator = coordinator
    app.state.election_manager = election_manager

    # --- Internal Peer RPC Endpoints ---

    @app.post("/propose", summary="Receive PROPOSE from Leader")
    async def handle_propose(msg: ProposeMessage):
        """Follower receives proposal, logs it as PROPOSED, and replies with ACK."""
        logger.info(
            f"[Node {node_id}] Received PROPOSE from Leader Node {msg.from_node}: "
            f"epoch={msg.epoch}, seq={msg.seq}, op={msg.op}, value={msg.value}"
        )
        # Log proposal locally
        log_store.append(
            epoch=msg.epoch,
            seq=msg.seq,
            op=msg.op,
            value=msg.value,
            status="PROPOSED",
        )
        return {
            "ack": True,
            "epoch": msg.epoch,
            "seq": msg.seq,
            "from_node": node_id,
        }

    @app.post("/commit", summary="Receive COMMIT from Leader")
    async def handle_commit(msg: CommitMessage):
        """Follower applies commit to local log, making new target visible."""
        logger.info(
            f"[Node {node_id}] Received COMMIT: epoch={msg.epoch}, seq={msg.seq}"
        )
        success = log_store.commit(epoch=msg.epoch, seq=msg.seq)
        return {"committed": success, "epoch": msg.epoch, "seq": msg.seq}

    @app.post("/heartbeat", summary="Receive HEARTBEAT from Leader")
    async def handle_heartbeat(msg: HeartbeatMessage):
        """Process periodic heartbeat from leader."""
        return election_manager.handle_heartbeat(epoch=msg.epoch, from_node=msg.from_node)

    @app.post("/election", summary="Receive ELECTION message from Candidate")
    async def handle_election(msg: ElectionMessage):
        """Evaluate candidate credentials and return vote."""
        return election_manager.handle_election_request(
            cand_epoch=msg.epoch,
            cand_seq=msg.seq,
            cand_id=msg.from_node,
        )

    @app.post("/sync_logs", summary="Synchronize logs during recovery")
    async def handle_sync_logs(msg: SyncLogsRequest):
        """Apply reconciled logs from newly elected leader."""
        logger.info(f"[Node {node_id}] Applying log synchronization with {len(msg.entries)} entries.")
        log_store.sync_entries(msg.entries)
        return {"status": "ok", "synced_count": len(msg.entries)}

    @app.post("/forward", summary="Forward client request to Leader")
    async def handle_forward(msg: ForwardMessage):
        """Leader processes forwarded Write or Guess from follower."""
        if election_manager.role != "LEADER":
            # If this node is not the leader either, attempt forward to current known leader
            leader_id = election_manager.current_leader
            if leader_id and leader_id != node_id and leader_id in NODES:
                leader = NODES[leader_id]
                try:
                    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                        resp = await client.post(
                            f"http://{leader['host']}:{leader['port']}/forward",
                            json=msg.dict(),
                        )
                        return resp.json()
                except Exception as e:
                    raise HTTPException(status_code=503, detail=f"Cannot reach leader Node {leader_id}: {e}")
            raise HTTPException(status_code=503, detail="Leader is currently unavailable.")

        if msg.op == "WRITE":
            res = await coordinator.propose_and_commit(op="WRITE", value=msg.value)
            return {"forwarded": True, "result": res}
        elif msg.op == "GUESS":
            res = coordinator.record_guess(client_id=msg.client_id, value=msg.value)
            return {"forwarded": True, "result": res}
        else:
            raise HTTPException(status_code=400, detail=f"Unknown forwarded op: {msg.op}")

    # --- Client-Facing Endpoints ---

    @app.post("/write", summary="Client Write: Set game target")
    async def handle_write(req: WriteRequest):
        """Client sets target (0-100). If follower, forwards to leader."""
        if election_manager.role == "LEADER":
            logger.info(f"[Node {node_id} (Leader)] Handling client Write({req.value}) from {req.client_id}")
            result = await coordinator.propose_and_commit(op="WRITE", value=req.value)
            return {
                "handled_by": node_id,
                "role": "LEADER",
                "client_id": req.client_id,
                "value": req.value,
                "result": result,
            }
        else:
            # Follower: Forward to leader
            leader_id = election_manager.current_leader
            if not leader_id or leader_id not in NODES:
                raise HTTPException(status_code=503, detail="No active leader known to forward request.")

            leader = NODES[leader_id]
            logger.info(f"[Node {node_id} (Follower)] Forwarding Write({req.value}) to Leader Node {leader_id}")
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT + 2.0) as client:
                    resp = await client.post(
                        f"http://{leader['host']}:{leader['port']}/forward",
                        json={"op": "WRITE", "value": req.value, "client_id": req.client_id},
                    )
                    return {
                        "handled_by": node_id,
                        "forwarded_to": leader_id,
                        "role": "FOLLOWER",
                        "response": resp.json(),
                    }
            except Exception as e:
                raise HTTPException(status_code=503, detail=f"Failed to forward to leader Node {leader_id}: {e}")

    @app.post("/guess", summary="Client Guess: Submit a guess")
    async def handle_guess(req: GuessRequest):
        """Client submits guess. If follower, forwards to leader."""
        if election_manager.role == "LEADER":
            logger.info(f"[Node {node_id} (Leader)] Handling client Guess({req.value}) from {req.client_id}")
            rec = coordinator.record_guess(client_id=req.client_id, value=req.value)
            resolve_res = None
            if req.resolve_immediately:
                resolve_res = await coordinator.resolve_round()
            return {
                "handled_by": node_id,
                "role": "LEADER",
                "record": rec,
                "resolved": resolve_res,
            }
        else:
            # Follower: Forward to leader
            leader_id = election_manager.current_leader
            if not leader_id or leader_id not in NODES:
                raise HTTPException(status_code=503, detail="No active leader known.")

            leader = NODES[leader_id]
            logger.info(f"[Node {node_id} (Follower)] Forwarding Guess({req.value}) to Leader Node {leader_id}")
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT + 2.0) as client:
                    resp = await client.post(
                        f"http://{leader['host']}:{leader['port']}/forward",
                        json={"op": "GUESS", "value": req.value, "client_id": req.client_id},
                    )
                    return {
                        "handled_by": node_id,
                        "forwarded_to": leader_id,
                        "role": "FOLLOWER",
                        "response": resp.json(),
                    }
            except Exception as e:
                raise HTTPException(status_code=503, detail=f"Failed to forward guess to leader Node {leader_id}: {e}")

    @app.post("/resolve_round", summary="Evaluate round and commit closest guess")
    async def handle_resolve_round():
        """Leader resolves round: selects closest guess and commits it. Forwarded if follower."""
        if election_manager.role == "LEADER":
            res = await coordinator.resolve_round()
            return {"handled_by": node_id, "role": "LEADER", "result": res}
        else:
            leader_id = election_manager.current_leader
            if not leader_id or leader_id not in NODES:
                raise HTTPException(status_code=503, detail="No active leader known.")
            leader = NODES[leader_id]
            try:
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT + 2.0) as client:
                    resp = await client.post(f"http://{leader['host']}:{leader['port']}/resolve_round")
                    return {
                        "handled_by": node_id,
                        "forwarded_to": leader_id,
                        "role": "FOLLOWER",
                        "response": resp.json(),
                    }
            except Exception as e:
                raise HTTPException(status_code=503, detail=f"Failed to resolve round on leader: {e}")

    @app.get("/target", summary="Get current committed target")
    async def get_target():
        """Returns the current committed target value."""
        latest = log_store.get_latest_committed()
        return {
            "node_id": node_id,
            "target": latest["value"] if latest else None,
            "target_epoch": latest["epoch"] if latest else None,
            "target_seq": latest["seq"] if latest else None,
            "has_target": latest is not None,
        }

    @app.get("/guesses", summary="Get pending guesses for current round")
    async def get_guesses():
        """Returns pending guesses if leader."""
        return {
            "node_id": node_id,
            "role": election_manager.role,
            "guesses": coordinator.round_guesses if election_manager.role == "LEADER" else [],
        }

    @app.get("/log", summary="Get full local transaction log")
    async def get_log():
        """Returns all persistent log entries."""
        return {
            "node_id": node_id,
            "entries": log_store.get_all_entries(),
        }

    @app.get("/status", summary="Get node status and topology state")
    async def get_status():
        """Returns full node health, role, epoch, seq, and current target."""
        latest_committed = log_store.get_latest_committed()
        latest_entry = log_store.get_latest_entry()
        return {
            "node_id": node_id,
            "role": election_manager.role,
            "current_leader": election_manager.current_leader,
            "epoch": election_manager.epoch,
            "seq": coordinator.seq if election_manager.role == "LEADER" else (latest_entry["seq"] if latest_entry else 0),
            "current_target": latest_committed["value"] if latest_committed else None,
            "latest_log": latest_entry,
            "log_count": len(log_store.get_all_entries()),
            "pending_guesses_count": len(coordinator.round_guesses) if election_manager.role == "LEADER" else 0,
        }

    @app.get("/", response_class=HTMLResponse, summary="Interactive Cluster Dashboard")
    async def dashboard():
        """Serves rich visual dashboard for demonstration and live defense."""
        return HTMLResponse(content=get_dashboard_html(node_id))

    return app


def get_dashboard_html(active_node_id: int) -> str:
    """Returns self-contained modern HTML/CSS/JS dashboard."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ZooKeeper System Lab Dashboard - Node {active_node_id}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg: #0b0f19;
            --surface: #131b2e;
            --surface-hover: #1a243d;
            --border: #233152;
            --primary: #3b82f6;
            --primary-glow: rgba(59, 130, 246, 0.25);
            --leader: #f59e0b;
            --leader-glow: rgba(245, 158, 11, 0.25);
            --follower: #10b981;
            --follower-glow: rgba(16, 185, 129, 0.25);
            --candidate: #ec4899;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --code-bg: #070b13;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            background: var(--bg);
            color: var(--text-main);
            font-family: 'Inter', -apple-system, sans-serif;
            min-height: 100vh;
            padding: 24px;
        }}
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border);
            padding-bottom: 16px;
            margin-bottom: 24px;
        }}
        .logo-group {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .badge {{
            padding: 4px 10px;
            border-radius: 9999px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .badge-leader {{ background: var(--leader-glow); color: var(--leader); border: 1px solid var(--leader); }}
        .badge-follower {{ background: var(--follower-glow); color: var(--follower); border: 1px solid var(--follower); }}
        .badge-candidate {{ background: rgba(236, 72, 153, 0.2); color: var(--candidate); border: 1px solid var(--candidate); }}

        .grid-nodes {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .node-card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 16px;
            transition: all 0.2s ease;
            position: relative;
            overflow: hidden;
        }}
        .node-card.is-active-card {{
            border-color: var(--primary);
            box-shadow: 0 0 20px var(--primary-glow);
        }}
        .node-card.is-leader {{
            border-color: var(--leader);
            box-shadow: 0 0 20px var(--leader-glow);
        }}
        .node-title {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 12px;
            font-weight: 700;
            font-size: 15px;
        }}
        .node-metric {{
            font-size: 13px;
            color: var(--text-muted);
            margin-bottom: 6px;
            display: flex;
            justify-content: space-between;
        }}
        .metric-val {{
            font-family: 'JetBrains Mono', monospace;
            color: var(--text-main);
            font-weight: 600;
        }}

        .main-panel {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 24px;
        }}
        @media (max-width: 900px) {{
            .main-panel {{ grid-template-columns: 1fr; }}
        }}

        .card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
        }}
        .card h2 {{
            font-size: 16px;
            font-weight: 700;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .form-row {{
            margin-bottom: 16px;
        }}
        label {{
            display: block;
            font-size: 13px;
            color: var(--text-muted);
            margin-bottom: 6px;
            font-weight: 500;
        }}
        input, select {{
            width: 100%;
            background: var(--code-bg);
            border: 1px solid var(--border);
            color: var(--text-main);
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 14px;
            outline: none;
            transition: border-color 0.2s;
        }}
        input:focus, select:focus {{
            border-color: var(--primary);
        }}
        .btn {{
            background: var(--primary);
            color: white;
            border: none;
            padding: 10px 18px;
            border-radius: 8px;
            font-weight: 600;
            font-size: 14px;
            cursor: pointer;
            transition: all 0.2s;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }}
        .btn:hover {{ opacity: 0.9; transform: translateY(-1px); }}
        .btn-gold {{ background: var(--leader); color: #000; }}
        .btn-green {{ background: var(--follower); color: #000; }}

        .target-banner {{
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.15), rgba(245, 158, 11, 0.15));
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
            text-align: center;
            margin-bottom: 24px;
        }}
        .target-display {{
            font-family: 'JetBrains Mono', monospace;
            font-size: 48px;
            font-weight: 800;
            color: var(--leader);
            letter-spacing: 2px;
            margin: 8px 0;
        }}

        .log-table {{
            width: 100%;
            border-collapse: collapse;
            font-family: 'JetBrains Mono', monospace;
            font-size: 12px;
            margin-top: 12px;
        }}
        .log-table th, .log-table td {{
            padding: 8px 10px;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }}
        .log-table th {{
            color: var(--text-muted);
            font-weight: 600;
            background: rgba(255,255,255,0.02);
        }}
        .tag-committed {{ color: var(--follower); font-weight: 700; }}
        .tag-proposed {{ color: var(--leader); font-weight: 700; }}
    </style>
</head>
<body>
    <header>
        <div class="logo-group">
            <h1 style="font-size: 20px; font-weight: 800;">ZooKeeper Consensus Lab</h1>
            <span class="badge" id="myRoleBadge">Loading...</span>
        </div>
        <div style="font-size: 13px; color: var(--text-muted);">
            Connected to <strong>Node {active_node_id}</strong> (Port {NODES[active_node_id]['port']})
        </div>
    </header>

    <div class="target-banner">
        <div style="font-size: 13px; text-transform: uppercase; font-weight: 700; letter-spacing: 1px; color: var(--text-muted);">Current Game Target</div>
        <div class="target-display" id="currentTargetDisplay">--</div>
        <div style="font-size: 12px; color: var(--text-muted);" id="targetMetadata">Synchronized via ZAB Consensus Quorum</div>
    </div>

    <!-- 5 Nodes Status Cards -->
    <h3 style="font-size: 14px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-muted); margin-bottom: 12px;">Cluster Topology (5 Nodes, Quorum = 3)</h3>
    <div class="grid-nodes" id="nodesContainer">
        <!-- Rendered by JS -->
    </div>

    <!-- Operations Panel -->
    <div class="main-panel">
        <div class="card">
            <h2>🎮 Client Actions (Send to Node {active_node_id})</h2>
            <div style="font-size: 13px; color: var(--text-muted); margin-bottom: 16px;">
                Requests are automatically forwarded to the leader if Node {active_node_id} is a follower.
            </div>

            <!-- Write Form -->
            <div class="form-row">
                <label>Set Target (Write 0–100)</label>
                <div style="display: flex; gap: 8px;">
                    <input type="number" id="writeVal" min="0" max="100" placeholder="e.g. 50">
                    <button class="btn" onclick="sendWrite()">Write Target</button>
                </div>
            </div>

            <!-- Guess Form -->
            <div class="form-row">
                <label>Submit Guess</label>
                <div style="display: flex; gap: 8px; margin-bottom: 8px;">
                    <input type="text" id="clientId" placeholder="Client Name (e.g. Alice)" style="width: 45%;">
                    <input type="number" id="guessVal" min="0" max="100" placeholder="Guess (e.g. 62)" style="width: 55%;">
                </div>
                <button class="btn btn-green" onclick="sendGuess()">Submit Guess</button>
            </div>

            <!-- Resolve Form -->
            <div class="form-row" style="margin-top: 20px; border-top: 1px solid var(--border); padding-top: 16px;">
                <label>Evaluate Round (Leader selects closest guess)</label>
                <button class="btn btn-gold" onclick="resolveRound()">Evaluate & Commit Closest Guess</button>
            </div>
            
            <div id="actionResult" style="margin-top: 12px; font-size: 13px; font-family: 'JetBrains Mono', monospace; padding: 10px; background: var(--code-bg); border-radius: 8px; display: none;"></div>
        </div>

        <div class="card">
            <h2>📜 Local Persistent Log (Node {active_node_id})</h2>
            <div style="max-height: 380px; overflow-y: auto;">
                <table class="log-table">
                    <thead>
                        <tr>
                            <th>Epoch</th>
                            <th>Seq</th>
                            <th>Op</th>
                            <th>Value</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody id="logTbody">
                        <!-- Rendered by JS -->
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <script>
        const ACTIVE_ID = {active_node_id};
        const NODES_CONFIG = {NODES};

        async function fetchCluster() {{
            try {{
                // Fetch status from active node
                const statusRes = await fetch('/status');
                const myStatus = await statusRes.json();
                
                // Update badge & target
                const roleBadge = document.getElementById('myRoleBadge');
                roleBadge.textContent = myStatus.role;
                roleBadge.className = 'badge ' + (myStatus.role === 'LEADER' ? 'badge-leader' : (myStatus.role === 'FOLLOWER' ? 'badge-follower' : 'badge-candidate'));
                
                document.getElementById('currentTargetDisplay').textContent = myStatus.current_target !== null ? myStatus.current_target : 'NONE';
                document.getElementById('targetMetadata').textContent = `Epoch: ${{myStatus.epoch}} | Sequence: ${{myStatus.seq}} | Current Leader: Node ${{myStatus.current_leader || 'None'}}`;

                // Fetch logs
                const logRes = await fetch('/log');
                const logData = await logRes.json();
                renderLogs(logData.entries);

                // Fetch status of all 5 nodes
                const nodePromises = Object.values(NODES_CONFIG).map(n => 
                    fetch(`http://${{n.host}}:${{n.port}}/status`, {{ mode: 'cors' }})
                        .then(r => r.json())
                        .catch(() => ({{ node_id: n.id, is_down: true }}))
                );
                const allStatuses = await Promise.all(nodePromises);
                renderNodeCards(allStatuses);
            }} catch (err) {{
                console.error('Fetch error:', err);
            }}
        }}

        function renderNodeCards(statuses) {{
            const container = document.getElementById('nodesContainer');
            container.innerHTML = '';
            statuses.forEach(s => {{
                const card = document.createElement('div');
                card.className = 'node-card' + (s.node_id === ACTIVE_ID ? ' is-active-card' : '') + (s.role === 'LEADER' ? ' is-leader' : '');
                
                if (s.is_down) {{
                    card.innerHTML = `
                        <div class="node-title">
                            <span>Node ${{s.node_id}}</span>
                            <span class="badge" style="background: rgba(239, 68, 68, 0.2); color: #ef4444; border: 1px solid #ef4444;">OFFLINE</span>
                        </div>
                        <div style="font-size: 12px; color: var(--text-muted); margin-top: 10px;">Port: ${{NODES_CONFIG[s.node_id].port}}</div>
                    `;
                }} else {{
                    const roleClass = s.role === 'LEADER' ? 'badge-leader' : (s.role === 'FOLLOWER' ? 'badge-follower' : 'badge-candidate');
                    card.innerHTML = `
                        <div class="node-title">
                            <span>Node ${{s.node_id}} ${{s.node_id === ACTIVE_ID ? '<small>(Current)</small>' : ''}}</span>
                            <span class="badge ${{roleClass}}">${{s.role}}</span>
                        </div>
                        <div class="node-metric"><span>Port:</span><span class="metric-val">${{NODES_CONFIG[s.node_id].port}}</span></div>
                        <div class="node-metric"><span>Epoch:</span><span class="metric-val">${{s.epoch}}</span></div>
                        <div class="node-metric"><span>Seq:</span><span class="metric-val">${{s.seq}}</span></div>
                        <div class="node-metric"><span>Leader:</span><span class="metric-val">Node ${{s.current_leader || '-'}}</span></div>
                        <div class="node-metric"><span>Target:</span><span class="metric-val" style="color: var(--leader);">${{s.current_target !== null ? s.current_target : '-'}}</span></div>
                        <div class="node-metric"><span>Log Rows:</span><span class="metric-val">${{s.log_count}}</span></div>
                    `;
                }}
                container.appendChild(card);
            }});
        }}

        function renderLogs(entries) {{
            const tbody = document.getElementById('logTbody');
            tbody.innerHTML = '';
            if (!entries || entries.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--text-muted); padding: 16px;">Log is currently empty</td></tr>';
                return;
            }}
            entries.forEach(e => {{
                const tr = document.createElement('tr');
                const tagClass = e.status === 'COMMITTED' ? 'tag-committed' : 'tag-proposed';
                tr.innerHTML = `
                    <td>${{e.epoch}}</td>
                    <td>${{e.seq}}</td>
                    <td>${{e.op}}</td>
                    <td><strong>${{e.value}}</strong></td>
                    <td class="${{tagClass}}">${{e.status}}</td>
                `;
                tbody.appendChild(tr);
            }});
        }}

        function showResult(msg, isSuccess = true) {{
            const div = document.getElementById('actionResult');
            div.style.display = 'block';
            div.style.color = isSuccess ? '#10b981' : '#ef4444';
            div.textContent = typeof msg === 'object' ? JSON.stringify(msg, null, 2) : msg;
        }}

        async function sendWrite() {{
            const val = parseInt(document.getElementById('writeVal').value);
            if (isNaN(val) || val < 0 || val > 100) return alert('Enter integer between 0 and 100');
            try {{
                const res = await fetch('/write', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ value: val, client_id: 'WebDashboard' }})
                }});
                const data = await res.json();
                showResult(data, res.ok);
                fetchCluster();
            }} catch (err) {{ showResult(err.message, false); }}
        }}

        async function sendGuess() {{
            const client = document.getElementById('clientId').value.trim() || 'Client_Web';
            const val = parseInt(document.getElementById('guessVal').value);
            if (isNaN(val) || val < 0 || val > 100) return alert('Enter integer between 0 and 100');
            try {{
                const res = await fetch('/guess', {{
                    method: 'POST',
                    headers: {{ 'Content-Type': 'application/json' }},
                    body: JSON.stringify({{ value: val, client_id: client }})
                }});
                const data = await res.json();
                showResult(data, res.ok);
                fetchCluster();
            }} catch (err) {{ showResult(err.message, false); }}
        }}

        async function resolveRound() {{
            try {{
                const res = await fetch('/resolve_round', {{ method: 'POST' }});
                const data = await res.json();
                showResult(data, res.ok);
                fetchCluster();
            }} catch (err) {{ showResult(err.message, false); }}
        }}

        setInterval(fetchCluster, 1000);
        fetchCluster();
    </script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Start a ZooKeeper consensus node")
    parser.add_argument("--id", type=int, required=True, choices=[1, 2, 3, 4, 5], help="Node ID (1-5)")
    args = parser.parse_args()

    node_id = args.id
    node_cfg = NODES[node_id]
    app = create_app(node_id)

    print(f"Starting Node {node_id} on http://{node_cfg['host']}:{node_cfg['port']} ...")
    uvicorn.run(app, host=node_cfg["host"], port=node_cfg["port"], log_level="warning")


if __name__ == "__main__":
    main()
