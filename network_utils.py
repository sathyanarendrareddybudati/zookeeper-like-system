"""Network utilities for raw TCP socket communication.

Provides:
- send_rpc: sends a JSON-formatted request over TCP and receives a JSON response.
- Line-delimited JSON framing (\n) for message boundaries.
"""

import json
import logging
import socket
from typing import Any, Dict, Optional

logger = logging.getLogger("NetworkUtils")


def send_rpc(
    host: str,
    port: int,
    message: Dict[str, Any],
    timeout: float = 2.5,
) -> Optional[Dict[str, Any]]:
    """Send a JSON message over a raw TCP socket and return the parsed JSON response.

    Uses line-delimited JSON (ends with \\n) as specified by the lab message protocol.
    Returns None if connection fails, times out, or receives invalid data.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect((host, port))

            # Send line-delimited JSON message
            payload = (json.dumps(message) + "\n").encode("utf-8")
            sock.sendall(payload)

            # Read response using buffered line reader
            sock_file = sock.makefile("r", encoding="utf-8")
            response_line = sock_file.readline()
            if not response_line:
                return None

            return json.loads(response_line.strip())
    except (socket.timeout, ConnectionRefusedError, BrokenPipeError, OSError) as e:
        logger.debug(f"RPC to {host}:{port} failed: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON response from {host}:{port}: {e}")
        return None
    except Exception as e:
        logger.debug(f"Unexpected error in send_rpc to {host}:{port}: {e}")
        return None
