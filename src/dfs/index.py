"""Materialized SQLite index over path records.

The index is derived and disposable (design doc §4): the durable metadata is
the append-only JSONL log; this database exists for fast queries and can be
deleted and rebuilt at any time.
"""

import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS records (
    path     TEXT PRIMARY KEY,
    lamport  INTEGER NOT NULL,
    node     TEXT NOT NULL,
    state    TEXT NOT NULL CHECK (state IN ('live', 'tombstone')),
    hash     TEXT,
    size     INTEGER,
    mtime    TEXT
);
CREATE TABLE IF NOT EXISTS holders (
    path   TEXT NOT NULL,
    node   TEXT NOT NULL,
    PRIMARY KEY (path, node)
);
CREATE INDEX IF NOT EXISTS idx_records_hash ON records (hash);
"""


@dataclass
class Record:
    path: str
    lamport: int
    node: str
    state: str  # "live" | "tombstone"
    hash: Optional[str] = None
    size: Optional[int] = None
    mtime: Optional[str] = None

    def version_key(self) -> tuple[int, str]:
        return (self.lamport, self.node)

    def to_json(self) -> str:
        return json.dumps(
            {
                "path": self.path,
                "version": {"lamport": self.lamport, "node": self.node},
                "state": self.state,
                "hash": self.hash,
                "size": self.size,
                "mtime": self.mtime,
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, line: str) -> "Record":
        obj = json.loads(line)
        return cls(
            path=obj["path"],
            lamport=obj["version"]["lamport"],
            node=obj["version"]["node"],
            state=obj["state"],
            hash=obj.get("hash"),
            size=obj.get("size"),
            mtime=obj.get("mtime"),
        )


class Index:
    def __init__(self, db_path: Path | str):
        # The API serves requests from worker threads; serialize access with a lock.
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.Lock()
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def upsert(self, record: Record) -> bool:
        """Merge a record by version (LWW). Returns True if it won and was stored."""
        with self._lock:
            existing = self._get_unlocked(record.path)
            if existing is not None and existing.version_key() >= record.version_key():
                return False
            self._conn.execute(
                "INSERT OR REPLACE INTO records (path, lamport, node, state, hash, size, mtime)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record.path, record.lamport, record.node, record.state,
                 record.hash, record.size, record.mtime),
            )
            self._conn.commit()
            return True

    def get(self, path: str) -> Optional[Record]:
        with self._lock:
            return self._get_unlocked(path)

    def _get_unlocked(self, path: str) -> Optional[Record]:
        row = self._conn.execute(
            "SELECT path, lamport, node, state, hash, size, mtime FROM records WHERE path = ?",
            (path,),
        ).fetchone()
        return Record(*row) if row else None

    def live_records(self) -> list[Record]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT path, lamport, node, state, hash, size, mtime FROM records"
                " WHERE state = 'live' ORDER BY path"
            ).fetchall()
        return [Record(*row) for row in rows]

    def by_hash(self, hash_: str) -> Optional[Record]:
        with self._lock:
            row = self._conn.execute(
                "SELECT path, lamport, node, state, hash, size, mtime FROM records"
                " WHERE hash = ? AND state = 'live' LIMIT 1",
                (hash_,),
            ).fetchone()
        return Record(*row) if row else None

    def set_holder(self, path: str, node: str, present: bool = True) -> None:
        with self._lock:
            if present:
                self._conn.execute(
                    "INSERT OR IGNORE INTO holders (path, node) VALUES (?, ?)", (path, node)
                )
            else:
                self._conn.execute(
                    "DELETE FROM holders WHERE path = ? AND node = ?", (path, node)
                )
            self._conn.commit()

    def holders(self, path: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT node FROM holders WHERE path = ? ORDER BY node", (path,)
            ).fetchall()
        return [r[0] for r in rows]
