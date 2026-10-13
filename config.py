"""Configuration for ZooKeeper-like System Lab.

Topology:
- 5 nodes running on localhost ports 5001-5005.
- Quorum is 3 (majority of 5).
- Node 1 is initial leader.
"""

from typing import Dict, Any

NODES: Dict[int, Dict[str, Any]] = {
    1: {"id": 1, "host": "127.0.0.1", "port": 5001},
    2: {"id": 2, "host": "127.0.0.1", "port": 5002},
    3: {"id": 3, "host": "127.0.0.1", "port": 5003},
    4: {"id": 4, "host": "127.0.0.1", "port": 5004},
    5: {"id": 5, "host": "127.0.0.1", "port": 5005},
}

TOTAL_NODES = len(NODES) # 5
QUORUM = (TOTAL_NODES // 2) + 1 # 3

INITIAL_LEADER_ID = 1

# Timing parameters (in seconds)
HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT_MIN = 2.5
HEARTBEAT_TIMEOUT_MAX = 4.0
ACK_TIMEOUT = 2.5
REQUEST_TIMEOUT = 2.0
