"""Write path (design doc §7).

1. Bytes buffer to /.dfs/tmp (the API endpoint or FUSE layer does this).
2. Before committing, check reachability: if the write threshold needs a
   second holder and no peer answers, raise IsolatedWriteError (EROFS) —
   no isolated edits.
3. Compute the BLAKE3 hash, assign a new version (lamport++), move the file
   into /data at its logical path, append the record to the meta log.
4. Push the file to reachable peers (`POST /v1/blob`), most free space first,
   until the write threshold (default 2 distinct holders) is met.
5. Return the committed record; record and holder updates gossip out.

Replicas are pushed with the record metadata in an `x-dfs-record` header
(base64 JSON, so non-ASCII paths survive HTTP header rules) and the raw bytes
as the request body. The receiving side of the push lives in api.py.
"""

import base64
import errno
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import httpx

from .auth import sign
from .config import Config
from .hashing import hash_file
from .index import Index, Record
from .metalog import MetaLog
from .peers import PeerStore

log = logging.getLogger("dfs.writer")


class IsolatedWriteError(OSError):
    """No peer reachable: the write is refused before touching /data (EROFS)."""

    def __init__(self, path: str):
        super().__init__(errno.EROFS, "no peer reachable, refusing isolated edit", path)


class WriteThresholdError(IOError):
    """Committed locally but could not reach enough holders for the threshold."""


def encode_record_header(record: Record) -> str:
    return base64.b64encode(record.to_json().encode()).decode("ascii")


def decode_record_header(value: str) -> Record:
    return Record.from_dict(json.loads(base64.b64decode(value)))


def _default_client_factory(base_url: str) -> httpx.Client:
    return httpx.Client(base_url=base_url, timeout=httpx.Timeout(30, read=None))


def push_blob(
    client_factory: Callable[[str], httpx.Client],
    url: str,
    cluster_secret: str,
    record: Record,
    local_path: Path,
) -> dict:
    """Push a copy of `record`'s bytes to the agent at `url` (POST /v1/blob)."""
    headers = {
        "x-dfs-token": sign(cluster_secret, "POST", "/v1/blob"),
        "x-dfs-record": encode_record_header(record),
    }
    with client_factory(url) as client:
        with local_path.open("rb") as fh:
            resp = client.post("/v1/blob", headers=headers, content=fh)
        resp.raise_for_status()
        return resp.json()


class Writer:
    def __init__(
        self,
        config: Config,
        index: Index,
        metalog: MetaLog,
        peers: PeerStore,
        client_factory: Callable[[str], httpx.Client] | None = None,
    ):
        self.config = config
        self.index = index
        self.metalog = metalog
        self.peers = peers
        # Injectable for tests (a TestClient is an httpx.Client).
        self._client_factory = client_factory or _default_client_factory

    def buffer(self) -> Path:
        """A fresh buffer file under /.dfs/tmp for incoming write bytes."""
        return self.config.tmp_dir / f"write-{uuid.uuid4().hex}"

    def reachable_peers(self) -> list[dict]:
        """Peers answering /v1/health right now, most free space first."""
        alive = []
        for url in self.peers.urls():
            try:
                with self._client_factory(url) as client:
                    resp = client.get(
                        "/v1/health",
                        headers={"x-dfs-token": sign(self.config.cluster_secret, "GET", "/v1/health")},
                    )
                    resp.raise_for_status()
                    health = resp.json()
            except (httpx.HTTPError, OSError, ValueError) as exc:
                log.debug("peer %s unreachable: %s", url, exc)
                continue
            if node := health.get("node"):
                self.peers.note_node(url, node)
            alive.append({"url": url, "node": health.get("node"),
                          "free_bytes": health.get("free_bytes", 0)})
        alive.sort(key=lambda p: p["free_bytes"], reverse=True)
        return alive

    def write(self, path: str, buffered: Path) -> Record:
        """Commit buffered bytes as the new version of `path` and replicate.

        Raises IsolatedWriteError before committing if the threshold needs a
        peer and none is reachable. Raises WriteThresholdError if the local
        commit landed but replication fell short (the commit stays: it will
        gossip out and the reconciler tops it up when peers return).
        """
        needed_remote = self.config.write_threshold - 1
        peers_up = self.reachable_peers() if needed_remote > 0 else []
        if needed_remote > 0 and not peers_up:
            buffered.unlink(missing_ok=True)
            raise IsolatedWriteError(path)

        record = Record(
            path=path,
            lamport=self.index.max_lamport() + 1,
            node=self.config.node_id,
            state="live",
            hash=hash_file(buffered),
            size=buffered.stat().st_size,
            mtime=datetime.now(timezone.utc).isoformat(),
        )
        dest = self.config.data_dir / path.lstrip("/")
        dest.parent.mkdir(parents=True, exist_ok=True)
        buffered.replace(dest)
        self.index.upsert(record)
        self.metalog.append(record)
        self.index.set_holder(path, self.config.node_id)
        # A newly written version supersedes any stale cached copy of the path.
        (self.config.cache_dir / path.lstrip("/")).unlink(missing_ok=True)

        confirmed = 1  # ourselves
        for peer in peers_up:
            if confirmed >= self.config.write_threshold:
                break
            try:
                push_blob(self._client_factory, peer["url"], self.config.cluster_secret,
                          record, dest)
            except (httpx.HTTPError, OSError, ValueError) as exc:
                log.warning("push of %s to %s failed: %s", path, peer["url"], exc)
                continue
            if peer["node"]:
                self.index.set_holder(path, peer["node"])
            confirmed += 1
        if confirmed < self.config.write_threshold:
            raise WriteThresholdError(
                f"{path}: committed locally but only {confirmed} of "
                f"{self.config.write_threshold} holders confirmed"
            )
        log.info("wrote %s (%d bytes) to %d holders", path, record.size or 0, confirmed)
        return record


def receive_blob(config: Config, index: Index, metalog: MetaLog,
                 record: Record, buffered: Path) -> None:
    """Receiving side of a push: verify, place under /data, index, log.

    A pushed copy is a deliberate placement (design doc §9), so it lands in
    /data rather than the cache. Raises ValueError on hash mismatch.
    """
    got = hash_file(buffered)
    if got != record.hash:
        buffered.unlink(missing_ok=True)
        raise ValueError(f"hash mismatch: expected {record.hash}, got {got}")
    dest = config.data_dir / record.path.lstrip("/")
    dest.parent.mkdir(parents=True, exist_ok=True)
    buffered.replace(dest)
    if index.upsert(record):
        metalog.append(record)
    index.set_holder(record.path, config.node_id)
