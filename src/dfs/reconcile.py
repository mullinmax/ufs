"""Reconciler loop (design doc §8): top up copies to N in the background.

Each round scans the index for live paths this node physically holds whose
known holder count is below N, and pushes copies to reachable peers that do
not already hold them, most free space first (capacity-based placement, no
anchor role). Holder updates then gossip out as usual.

Only paths with local bytes are acted on: some other holder runs the same
loop for the rest, so the cluster converges without coordination.
"""

import asyncio
import logging
from pathlib import Path
from typing import Callable, Optional

import httpx

from .config import Config
from .index import Index
from .peers import PeerStore
from .writer import Writer, push_blob

log = logging.getLogger("dfs.reconcile")


class Reconciler:
    def __init__(
        self,
        config: Config,
        index: Index,
        writer: Writer,
        peers: PeerStore,
        client_factory: Callable[[str], httpx.Client] | None = None,
    ):
        self.config = config
        self.index = index
        self.writer = writer
        self.peers = peers
        self._client_factory = client_factory or writer._client_factory

    def _local_copy(self, path: str) -> Optional[Path]:
        for base in (self.config.data_dir, self.config.cache_dir):
            candidate = base / path.lstrip("/")
            if candidate.is_file():
                return candidate
        return None

    def round(self) -> int:
        """One reconciliation pass. Returns the number of copies pushed."""
        peers_up = self.writer.reachable_peers()
        if not peers_up:
            return 0
        pushed = 0
        for record in self.index.live_records():
            holders = self.index.holders(record.path)
            need = self.config.n_copies - len(holders)
            if need <= 0 or self.config.node_id not in holders:
                continue
            source = self._local_copy(record.path)
            if source is None:
                continue
            for peer in peers_up:
                if need <= 0:
                    break
                if peer["node"] is None or peer["node"] in holders:
                    continue
                try:
                    push_blob(self._client_factory, peer["url"],
                              self.config.cluster_secret, record, source)
                except (httpx.HTTPError, OSError, ValueError) as exc:
                    log.warning("reconcile push of %s to %s failed: %s",
                                record.path, peer["url"], exc)
                    continue
                self.index.set_holder(record.path, peer["node"])
                holders.append(peer["node"])
                need -= 1
                pushed += 1
        if pushed:
            log.info("reconciler pushed %d copies", pushed)
        return pushed

    async def run(self) -> None:
        log.info("reconciler loop started (interval %.0fs, N=%d)",
                 self.config.reconcile_interval, self.config.n_copies)
        while True:
            try:
                await asyncio.to_thread(self.round)
            except Exception as exc:
                log.warning("reconcile round failed: %s", exc)
            await asyncio.sleep(self.config.reconcile_interval)
