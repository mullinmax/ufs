"""Anti-entropy gossip (design doc §13).

Each round, for every known peer:
- pull: `GET /v1/index?since=<cursor>` and merge the returned records (LWW)
  and holder entries into the local index; remote records that win the merge
  are appended to the local meta log so they are durable here too.
- push: `POST /v1/index` with our own deltas since what we last pushed.

Cursors are the *peer's* local sequence numbers (pull) and *our* local
sequence numbers (push), tracked per peer URL in `/.dfs/gossip_cursors.json`.
No consensus, no quorum: nodes converge by exchanging deltas and merging by
version.
"""

import asyncio
import json
import logging
from pathlib import Path

import httpx

from .auth import sign
from .config import Config
from .delete import apply_remote_record
from .index import Index, Record
from .metalog import MetaLog
from .peers import PeerStore

log = logging.getLogger("dfs.gossip")


def merge_delta(config: Config, index: Index, metalog: MetaLog,
                records: list[dict], holders: list[dict]) -> int:
    """Merge a gossiped delta into the local index. Returns records accepted.

    Every remote record that wins the merge also applies its §10 side
    effects: a tombstone purges local bytes and holders; a newer live
    record drops any stale local copy (straggler reconciliation). Holder
    entries for tombstoned paths are stale gossip and are skipped.
    """
    accepted = 0
    for obj in records:
        record = Record.from_dict(obj)
        if index.upsert(record):
            metalog.append(record)
            apply_remote_record(config, index, record)
            accepted += 1
    for entry in holders:
        current = index.get(entry["path"])
        if current is not None and current.state == "tombstone":
            continue
        index.set_holder(entry["path"], entry["node"])
    return accepted


class Gossip:
    def __init__(
        self,
        config: Config,
        index: Index,
        metalog: MetaLog,
        peers: PeerStore,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.config = config
        self.index = index
        self.metalog = metalog
        self.peers = peers
        self._transport = transport  # injectable for tests (ASGITransport)
        self._cursors_path = config.gossip_cursors_path
        self._cursors: dict[str, dict] = self._load_cursors()

    def _load_cursors(self) -> dict:
        if self._cursors_path.exists():
            try:
                return json.loads(self._cursors_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                log.warning("could not read gossip cursors; starting from zero")
        return {}

    def _save_cursors(self) -> None:
        self._cursors_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._cursors_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._cursors, sort_keys=True, indent=2), encoding="utf-8")
        tmp.replace(self._cursors_path)

    def _headers(self, method: str, path: str) -> dict:
        return {"x-dfs-token": sign(self.config.cluster_secret, method, path)}

    async def sync_peer(self, url: str) -> None:
        cursors = self._cursors.setdefault(url, {"pull": 0, "push": 0})
        async with httpx.AsyncClient(
            base_url=url, transport=self._transport, timeout=30
        ) as client:
            # Pull the peer's deltas since our per-peer cursor and merge them.
            resp = await client.get(
                "/v1/index",
                params={"since": cursors["pull"]},
                headers=self._headers("GET", "/v1/index"),
            )
            resp.raise_for_status()
            data = resp.json()
            accepted = merge_delta(self.config, self.index, self.metalog,
                                   data["records"], data["holders"])
            cursors["pull"] = data["cursor"]
            if peer_node := data.get("node"):
                self.peers.note_node(url, peer_node)
            if accepted:
                log.info("pulled %d records from %s", accepted, url)

            # Push our deltas since what this peer last saw from us.
            records, holders, new_cursor = self.index.changes_since(cursors["push"])
            if records or holders:
                resp = await client.post(
                    "/v1/index",
                    json={
                        "node": self.config.node_id,
                        "records": [r.to_dict() for r in records],
                        "holders": holders,
                    },
                    headers=self._headers("POST", "/v1/index"),
                )
                resp.raise_for_status()
            cursors["push"] = new_cursor
        self._save_cursors()

    async def round(self) -> None:
        for url in self.peers.urls():
            try:
                await self.sync_peer(url)
            except Exception as exc:
                log.debug("gossip with %s failed: %s", url, exc)

    async def run(self) -> None:
        log.info("gossip loop started (interval %.0fs)", self.config.gossip_interval)
        while True:
            await self.round()
            await asyncio.sleep(self.config.gossip_interval)
