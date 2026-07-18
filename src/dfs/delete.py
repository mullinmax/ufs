"""Deletion and tombstones (design doc §10).

A delete writes a tombstone: a record with `state: "tombstone"` at a new
version (lamport++), then removes the local bytes and gossips the tombstone.
Other holders that receive a tombstone whose version beats their held version
remove their bytes too (see `apply_remote_record`, called from the gossip
merge). Tombstones are kept forever — they are tiny, and they are what lets a
straggler that returns after months learn a path was deleted and purge its
stale copy.

Deletes follow the same no-isolated-edits rule as writes (§7): a delete is
refused with EROFS when the write threshold needs a peer and none is
reachable. Beyond the guard, the tombstone is pushed eagerly to reachable
peers (`POST /v1/index`) so deletion takes effect cluster-wide without
waiting for a gossip round.
"""

import logging
from datetime import datetime, timezone

import httpx

from .auth import sign
from .config import Config
from .hashing import hash_file
from .index import Index, Record
from .metalog import MetaLog
from .writer import IsolatedWriteError, Writer, WriteThresholdError

log = logging.getLogger("dfs.delete")


def purge_path(config: Config, path: str) -> None:
    """Remove any local bytes (data and cache) for `path`."""
    for base in (config.data_dir, config.cache_dir):
        (base / path.lstrip("/")).unlink(missing_ok=True)


def apply_remote_record(config: Config, index: Index, record: Record) -> None:
    """Side effects after a gossiped record wins the local merge (§10).

    A winning tombstone purges local bytes and forgets the holder set (each
    node does the same when the tombstone reaches it). A winning live record
    means any local copy is stale — straggler reconciliation on rejoin — so
    stale bytes are dropped and this node stops claiming to be a holder; the
    reconciler or the next read re-fetches if needed. Bytes that already
    match the new version (verified by hash) are kept.
    """
    if record.state == "tombstone":
        purge_path(config, record.path)
        index.clear_holders(record.path)
        return
    still_holds = False
    for base in (config.data_dir, config.cache_dir):
        local = base / record.path.lstrip("/")
        if not local.is_file():
            continue
        if local.stat().st_size == record.size and hash_file(local) == record.hash:
            still_holds = True
            continue
        local.unlink()
    if not still_holds:
        index.set_holder(record.path, config.node_id, present=False)


class Deleter:
    def __init__(self, config: Config, index: Index, metalog: MetaLog, writer: Writer):
        self.config = config
        self.index = index
        self.metalog = metalog
        self.writer = writer

    def delete(self, path: str) -> Record:
        """Tombstone `path`, purge local bytes, and notify reachable peers.

        Raises FileNotFoundError if the path is not live, IsolatedWriteError
        before committing if the threshold needs a peer and none is reachable,
        and WriteThresholdError if the tombstone landed locally but too few
        peers acknowledged it (the tombstone stays: gossip finishes the job).
        """
        record = self.index.get(path)
        if record is None or record.state != "live":
            raise FileNotFoundError(path)

        needed_remote = self.config.write_threshold - 1
        peers_up = self.writer.reachable_peers() if needed_remote > 0 else []
        if needed_remote > 0 and not peers_up:
            raise IsolatedWriteError(path)

        tombstone = Record(
            path=path,
            lamport=self.index.max_lamport() + 1,
            node=self.config.node_id,
            state="tombstone",
            hash=record.hash,
            mtime=datetime.now(timezone.utc).isoformat(),
        )
        self.index.upsert(tombstone)
        self.metalog.append(tombstone)
        purge_path(self.config, path)
        self.index.clear_holders(path)

        confirmed = 1  # ourselves
        for peer in peers_up:
            try:
                with self.writer._client_factory(peer["url"]) as client:
                    resp = client.post(
                        "/v1/index",
                        json={"node": self.config.node_id,
                              "records": [tombstone.to_dict()], "holders": []},
                        headers={"x-dfs-token": sign(self.config.cluster_secret,
                                                     "POST", "/v1/index")},
                    )
                    resp.raise_for_status()
            except (httpx.HTTPError, OSError, ValueError) as exc:
                log.warning("tombstone push of %s to %s failed: %s", path, peer["url"], exc)
                continue
            confirmed += 1
        if confirmed < self.config.write_threshold:
            raise WriteThresholdError(
                f"{path}: tombstoned locally but only {confirmed} of "
                f"{self.config.write_threshold} nodes confirmed"
            )
        log.info("deleted %s (tombstone lamport=%d, %d nodes confirmed)",
                 path, tombstone.lamport, confirmed)
        return tombstone
