"""Scan the data directory and (re)build the local index.

Rebuild-from-disk (design doc §14): /data plus the meta log recover the full
live set. The scan replays the meta log first (versions, tombstones), then
walks /data for files not yet known, registering them as new live records.
"""

from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .hashing import hash_file
from .index import Index, Record
from .metalog import MetaLog


def scan(config: Config, index: Index, metalog: MetaLog) -> int:
    """Replay the meta log, then scan /data for unknown files. Returns files indexed."""
    lamport = 0
    for record in metalog.read_all():
        index.upsert(record)
        lamport = max(lamport, record.lamport)

    count = 0
    for file_path in sorted(config.data_dir.rglob("*")):
        if not file_path.is_file():
            continue
        logical = "/" + str(file_path.relative_to(config.data_dir))
        stat = file_path.stat()
        existing = index.get(logical)
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        if existing is not None and existing.state == "live" and existing.size == stat.st_size and existing.mtime == mtime:
            index.set_holder(logical, config.node_id)
            count += 1
            continue
        lamport += 1
        record = Record(
            path=logical,
            lamport=lamport,
            node=config.node_id,
            state="live",
            hash=hash_file(file_path),
            size=stat.st_size,
            mtime=mtime,
        )
        if index.upsert(record):
            metalog.append(record)
        index.set_holder(logical, config.node_id)
        count += 1
    return count
