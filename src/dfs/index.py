"""Materialized SQLite index over path records.

The index is derived and disposable (design doc §4): the durable metadata is
the append-only JSONL log; this database exists for fast queries and can be
deleted and rebuilt at any time.

For anti-entropy (design doc §13) every accepted change — a record winning the
LWW merge, or a holder appearing/disappearing — is stamped with a local
monotonic sequence number. `changes_since(cursor)` returns everything stamped
after a cursor, which is exactly what `GET /v1/index?since=` serves. The
sequence is local to this node and never gossiped as-is; peers each keep their
own cursor per peer.
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
    mtime    TEXT,
    seq      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS holders (
    path   TEXT NOT NULL,
    node   TEXT NOT NULL,
    seq    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (path, node)
);
CREATE TABLE IF NOT EXISTS local_state (
    key    TEXT PRIMARY KEY,
    value  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_records_hash ON records (hash);
CREATE INDEX IF NOT EXISTS idx_records_seq ON records (seq);
CREATE INDEX IF NOT EXISTS idx_holders_seq ON holders (seq);
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

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "version": {"lamport": self.lamport, "node": self.node},
            "state": self.state,
            "hash": self.hash,
            "size": self.size,
            "mtime": self.mtime,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_dict(cls, obj: dict) -> "Record":
        return cls(
            path=obj["path"],
            lamport=obj["version"]["lamport"],
            node=obj["version"]["node"],
            state=obj["state"],
            hash=obj.get("hash"),
            size=obj.get("size"),
            mtime=obj.get("mtime"),
        )

    @classmethod
    def from_json(cls, line: str) -> "Record":
        return cls.from_dict(json.loads(line))


class Index:
    def __init__(self, db_path: Path | str):
        # The API serves requests from worker threads; serialize access with a lock.
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.Lock()
        self._migrate()
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def _migrate(self) -> None:
        # The index is disposable; a pre-Phase-1 schema (no seq columns) is
        # dropped outright and rebuilt from the meta log by the next scan.
        for table in ("records", "holders"):
            cols = [r[1] for r in self._conn.execute(f"PRAGMA table_info({table})")]
            if cols and "seq" not in cols:
                self._conn.execute(f"DROP TABLE {table}")
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _next_seq(self) -> int:
        row = self._conn.execute(
            "SELECT value FROM local_state WHERE key = 'seq'"
        ).fetchone()
        seq = (row[0] if row else 0) + 1
        self._conn.execute(
            "INSERT OR REPLACE INTO local_state (key, value) VALUES ('seq', ?)", (seq,)
        )
        return seq

    def cursor(self) -> int:
        """Current local sequence high-water mark."""
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM local_state WHERE key = 'seq'"
            ).fetchone()
            return row[0] if row else 0

    def upsert(self, record: Record) -> bool:
        """Merge a record by version (LWW). Returns True if it won and was stored."""
        with self._lock:
            existing = self._get_unlocked(record.path)
            if existing is not None and existing.version_key() >= record.version_key():
                return False
            self._conn.execute(
                "INSERT OR REPLACE INTO records (path, lamport, node, state, hash, size, mtime, seq)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (record.path, record.lamport, record.node, record.state,
                 record.hash, record.size, record.mtime, self._next_seq()),
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

    def max_lamport(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT MAX(lamport) FROM records").fetchone()
            return row[0] or 0

    def set_holder(self, path: str, node: str, present: bool = True) -> bool:
        """Add or remove a holder. Returns True if this actually changed anything
        (only real changes get a new sequence number, so gossip echoes settle)."""
        with self._lock:
            exists = self._conn.execute(
                "SELECT 1 FROM holders WHERE path = ? AND node = ?", (path, node)
            ).fetchone() is not None
            if present and not exists:
                self._conn.execute(
                    "INSERT INTO holders (path, node, seq) VALUES (?, ?, ?)",
                    (path, node, self._next_seq()),
                )
            elif not present and exists:
                self._conn.execute(
                    "DELETE FROM holders WHERE path = ? AND node = ?", (path, node)
                )
            else:
                return False
            self._conn.commit()
            return True

    def holders(self, path: str) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT node FROM holders WHERE path = ? ORDER BY node", (path,)
            ).fetchall()
        return [r[0] for r in rows]

    def changes_since(self, cursor: int) -> tuple[list[Record], list[dict], int]:
        """Anti-entropy delta: (records, holder entries, new cursor) with seq > cursor."""
        with self._lock:
            record_rows = self._conn.execute(
                "SELECT path, lamport, node, state, hash, size, mtime FROM records"
                " WHERE seq > ? ORDER BY seq", (cursor,)
            ).fetchall()
            holder_rows = self._conn.execute(
                "SELECT path, node FROM holders WHERE seq > ? ORDER BY seq", (cursor,)
            ).fetchall()
            row = self._conn.execute(
                "SELECT value FROM local_state WHERE key = 'seq'"
            ).fetchone()
            new_cursor = row[0] if row else 0
        records = [Record(*row) for row in record_rows]
        holders = [{"path": p, "node": n} for p, n in holder_rows]
        return records, holders, new_cursor
