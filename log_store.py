"""Persistent LogStore implementation using SQLite.

Maintains local transaction log with schema:
- epoch: int
- seq: int
- op: string ('WRITE' or 'GUESS')
- value: int
- status: string ('PROPOSED' or 'COMMITTED')
"""

import os
import sqlite3
import threading
from typing import List, Optional, Dict, Any


class LogStore:
    def __init__(self, node_id: int, db_dir: str = "data"):
        self.node_id = node_id
        self.db_dir = db_dir
        os.makedirs(self.db_dir, exist_ok=True)
        self.db_path = os.path.join(self.db_dir, f"node_{node_id}.db")
        self.lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS logs (
                        epoch INTEGER NOT NULL,
                        seq INTEGER NOT NULL,
                        op TEXT NOT NULL,
                        value INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        PRIMARY KEY (epoch, seq)
                    );
                    """
                )
                conn.commit()

    def append(self, epoch: int, seq: int, op: str, value: int, status: str = "PROPOSED") -> Dict[str, Any]:
        """Append or update a proposal in the persistent log."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO logs (epoch, seq, op, value, status)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(epoch, seq) DO UPDATE SET
                        op=excluded.op,
                        value=excluded.value,
                        status=excluded.status
                    """,
                    (epoch, seq, op, value, status),
                )
                conn.commit()
        return {"epoch": epoch, "seq": seq, "op": op, "value": value, "status": status}

    def commit(self, epoch: int, seq: int) -> bool:
        """Update proposal status to COMMITTED."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE logs SET status = 'COMMITTED' WHERE epoch = ? AND seq = ?",
                    (epoch, seq),
                )
                conn.commit()
                return cursor.rowcount > 0

    def get_entry(self, epoch: int, seq: int) -> Optional[Dict[str, Any]]:
        """Fetch a specific log entry by epoch and seq."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT epoch, seq, op, value, status FROM logs WHERE epoch = ? AND seq = ?",
                    (epoch, seq),
                )
                row = cursor.fetchone()
                return dict(row) if row else None

    def get_latest_entry(self) -> Optional[Dict[str, Any]]:
        """Return entry with highest (epoch, seq), or None if log is empty."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT epoch, seq, op, value, status FROM logs ORDER BY epoch DESC, seq DESC LIMIT 1"
                )
                row = cursor.fetchone()
                return dict(row) if row else None

    def get_latest_committed(self) -> Optional[Dict[str, Any]]:
        """Return latest committed entry."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT epoch, seq, op, value, status FROM logs WHERE status = 'COMMITTED' ORDER BY epoch DESC, seq DESC LIMIT 1"
                )
                row = cursor.fetchone()
                return dict(row) if row else None

    def get_all_entries(self) -> List[Dict[str, Any]]:
        """Return all log entries ordered by epoch, seq."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT epoch, seq, op, value, status FROM logs ORDER BY epoch ASC, seq ASC"
                )
                return [dict(row) for row in cursor.fetchall()]

    def sync_entries(self, entries: List[Dict[str, Any]]) -> None:
        """Synchronize log entries during recovery."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                for entry in entries:
                    cursor.execute(
                        """
                        INSERT INTO logs (epoch, seq, op, value, status)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(epoch, seq) DO UPDATE SET
                            op=excluded.op,
                            value=excluded.value,
                            status=excluded.status
                        """,
                        (entry["epoch"], entry["seq"], entry["op"], entry["value"], entry["status"]),
                    )
                conn.commit()

    def clear(self) -> None:
        """Clear all entries (useful for testing)."""
        with self.lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM logs")
                conn.commit()
