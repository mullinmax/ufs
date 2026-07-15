"""Peer discovery and the cached last-known-peers list (design doc §5).

Peers come from three places, merged:
- the static `DFS_PEERS` list,
- the tailnet (Headscale peer list, via the local tailscale client — see mesh.py),
- a locally cached last-known-peers file (`/.dfs/peers.json`).

The store also remembers which node id answered at which URL, so the read path
can turn a holder node id into a URL to fetch from.
"""

import json
import logging
import threading
from pathlib import Path
from typing import Optional

log = logging.getLogger("dfs.peers")


class PeerStore:
    def __init__(self, path: Path, static_peers: list[str] | None = None):
        self._path = path
        self._lock = threading.Lock()
        # {url: {"node": node_id | None}}
        self._peers: dict[str, dict] = {url: {"node": None} for url in (static_peers or [])}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            cached = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            log.warning("could not read cached peers from %s", self._path)
            return
        for url, info in cached.get("peers", {}).items():
            self._peers.setdefault(url, {"node": None})
            if info.get("node"):
                self._peers[url]["node"] = info["node"]

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"peers": self._peers}, sort_keys=True, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    def add(self, url: str) -> None:
        with self._lock:
            if url not in self._peers:
                self._peers[url] = {"node": None}
                self._save()

    def note_node(self, url: str, node_id: str) -> None:
        """Record that node_id answered at url (learned from gossip or /v1/health)."""
        with self._lock:
            self._peers.setdefault(url, {"node": None})
            if self._peers[url]["node"] != node_id:
                self._peers[url]["node"] = node_id
                self._save()

    def urls(self) -> list[str]:
        with self._lock:
            return sorted(self._peers)

    def url_for_node(self, node_id: str) -> Optional[str]:
        with self._lock:
            for url, info in self._peers.items():
                if info.get("node") == node_id:
                    return url
        return None
