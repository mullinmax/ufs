"""Cache maintenance (design doc §9) and pin warmup (§11).

Redundancy-aware LRU eviction: when the cache exceeds `DFS_CACHE_SIZE`,
evict the least-recently-used cached file, subject to one hard rule —
never evict a file if doing so would drop its global holder count below N.
Pinned files are never evicted. Cache entries that are stale (tombstoned,
superseded version, or unknown to the index) are always fair game.

Pin warmup: every round, any live path under a pinned prefix that has no
local copy is proactively fetched so it is already here when opened.

Last-access times are kept in `cache_lru.json`; the file is derived state
(like the cache itself) — losing it just makes eviction order approximate
until entries are touched again.
"""

import asyncio
import json
import logging
import time
from pathlib import Path

from .config import Config
from .fetch import Fetcher
from .index import Index
from .pins import PinStore

log = logging.getLogger("dfs.cache")


class CacheManager:
    def __init__(self, config: Config, index: Index, pins: PinStore):
        self.config = config
        self.index = index
        self.pins = pins
        self._lru: dict[str, float] = self._load_lru()

    def _load_lru(self) -> dict[str, float]:
        path = self.config.cache_lru_path
        if not path.is_file():
            return {}
        try:
            return {k: float(v) for k, v in json.loads(path.read_text()).items()}
        except (ValueError, AttributeError):
            return {}

    def _save_lru(self) -> None:
        self.config.cache_lru_path.write_text(json.dumps(self._lru))

    def record_access(self, path: str) -> None:
        self._lru[path] = time.time()
        self._save_lru()

    def _cached_files(self) -> list[tuple[str, Path, int]]:
        """(logical path, file path, size) for everything under /.dfs/cache."""
        out = []
        root = self.config.cache_dir
        for file_path in sorted(root.rglob("*")):
            if file_path.is_file():
                logical = "/" + file_path.relative_to(root).as_posix()
                out.append((logical, file_path, file_path.stat().st_size))
        return out

    def usage(self) -> int:
        return sum(size for _, _, size in self._cached_files())

    def _protected(self, logical: str, size: int) -> bool:
        """True if this cache entry must not be evicted right now."""
        record = self.index.get(logical)
        if record is None or record.state != "live" or record.size != size:
            return False  # stale junk: always evictable
        if self.pins.is_pinned(logical):
            return True
        if (self.config.data_dir / logical.lstrip("/")).is_file():
            return False  # /data copy keeps holdership; the cache copy is redundant
        holders = self.index.holders(logical)
        if self.config.node_id in holders and len(holders) - 1 < self.config.n_copies:
            return True  # evicting would drop the global holder count below N
        return False

    def evict_if_needed(self) -> list[str]:
        """Evict LRU cache entries until usage fits DFS_CACHE_SIZE (0 = unbounded)."""
        if self.config.cache_size <= 0:
            return []
        files = self._cached_files()
        excess = sum(size for _, _, size in files) - self.config.cache_size
        if excess <= 0:
            return []
        files.sort(key=lambda f: self._lru.get(f[0], 0.0))
        evicted = []
        for logical, file_path, size in files:
            if excess <= 0:
                break
            if self._protected(logical, size):
                continue
            file_path.unlink(missing_ok=True)
            self._lru.pop(logical, None)
            if not (self.config.data_dir / logical.lstrip("/")).is_file():
                self.index.set_holder(logical, self.config.node_id, present=False)
            excess -= size
            evicted.append(logical)
        if evicted:
            self._save_lru()
            log.info("evicted %d cached files: %s", len(evicted), ", ".join(evicted))
        if excess > 0:
            log.warning("cache still %d bytes over budget: all remaining entries "
                        "are pinned or load-bearing for N", excess)
        return evicted

    def warm_pins(self, fetcher: Fetcher) -> int:
        """Proactively fetch pinned paths that have no local copy yet."""
        warmed = 0
        for record in self.index.live_records():
            if not self.pins.is_pinned(record.path):
                continue
            rel = record.path.lstrip("/")
            if (self.config.data_dir / rel).is_file() or \
                    (self.config.cache_dir / rel).is_file():
                continue
            try:
                fetcher.open_path(record.path)
            except (IOError, FileNotFoundError) as exc:
                log.warning("pin warmup of %s failed: %s", record.path, exc)
                continue
            warmed += 1
        if warmed:
            log.info("pin warmup fetched %d files", warmed)
        return warmed

    def round(self, fetcher: Fetcher) -> None:
        self.warm_pins(fetcher)
        self.evict_if_needed()

    async def run(self, fetcher: Fetcher) -> None:
        log.info("cache loop started (interval %.0fs, budget %d bytes%s)",
                 self.config.reconcile_interval, self.config.cache_size,
                 "" if self.config.cache_size else " = unbounded")
        while True:
            try:
                await asyncio.to_thread(self.round, fetcher)
            except Exception as exc:
                log.warning("cache round failed: %s", exc)
            await asyncio.sleep(self.config.reconcile_interval)
