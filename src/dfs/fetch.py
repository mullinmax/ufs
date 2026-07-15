"""Fetch-then-open reads across nodes (design doc §7, read path).

1. If this node holds the path locally under /data at the current version,
   serve from disk.
2. If a previously fetched copy sits in /.dfs/cache at the current size, serve it.
3. Otherwise look up holders in the index, pick a reachable one, fetch the
   whole blob over HTTP into /.dfs/tmp, verify its BLAKE3 hash, move it into
   /.dfs/cache, register ourselves as a holder (cached copies count toward N),
   and serve it.
4. If no holder is reachable, raise (the FUSE layer maps this to EIO).
"""

import logging
import uuid
from pathlib import Path
from typing import Callable

import httpx

from .auth import sign
from .config import Config
from .hashing import hash_file
from .index import Index, Record
from .peers import PeerStore

log = logging.getLogger("dfs.fetch")


def _default_client_factory(base_url: str) -> httpx.Client:
    return httpx.Client(base_url=base_url, timeout=httpx.Timeout(30, read=None))


class Fetcher:
    def __init__(
        self,
        config: Config,
        index: Index,
        peers: PeerStore,
        client_factory: Callable[[str], httpx.Client] | None = None,
    ):
        self.config = config
        self.index = index
        self.peers = peers
        # Injectable for tests (a TestClient is an httpx.Client).
        self._client_factory = client_factory or _default_client_factory

    def open_path(self, path: str) -> Path:
        """Return a local filesystem path holding the current bytes of `path`."""
        record = self.index.get(path)
        if record is None or record.state != "live":
            raise FileNotFoundError(path)

        local = self.config.data_dir / path.lstrip("/")
        if local.is_file() and local.stat().st_size == record.size:
            return local

        cached = self.config.cache_dir / path.lstrip("/")
        if cached.is_file() and cached.stat().st_size == record.size:
            return cached

        for holder in self.index.holders(path):
            if holder == self.config.node_id:
                continue
            url = self.peers.url_for_node(holder)
            if url is None:
                continue
            try:
                self._fetch_blob(url, record, cached)
            except (httpx.HTTPError, OSError, ValueError) as exc:
                log.warning("fetch of %s from %s (%s) failed: %s", path, holder, url, exc)
                continue
            self.index.set_holder(path, self.config.node_id)
            log.info("fetched %s (%d bytes) from %s into cache", path, record.size or 0, holder)
            return cached

        raise IOError(f"no reachable holder for {path}")

    def _fetch_blob(self, url: str, record: Record, dest: Path) -> None:
        blob_path = f"/v1/blob/{record.hash}"
        tmp = self.config.tmp_dir / f"fetch-{uuid.uuid4().hex}"
        try:
            with self._client_factory(url) as client:
                headers = {"x-dfs-token": sign(self.config.cluster_secret, "GET", blob_path)}
                with client.stream("GET", blob_path, headers=headers) as resp:
                    resp.raise_for_status()
                    with tmp.open("wb") as fh:
                        for chunk in resp.iter_bytes():
                            fh.write(chunk)
            got = hash_file(tmp)
            if got != record.hash:
                raise ValueError(f"hash mismatch: expected {record.hash}, got {got}")
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp.replace(dest)
        finally:
            tmp.unlink(missing_ok=True)
