"""Agent configuration, read from environment variables (see design doc §15)."""

import os
from dataclasses import dataclass, field
from pathlib import Path


_SIZE_UNITS = {
    "": 1, "B": 1,
    "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12,
    "KIB": 2**10, "MIB": 2**20, "GIB": 2**30, "TIB": 2**40,
}


def parse_size(value: str) -> int:
    """Parse a human size like "4TB", "500GiB", or "1048576" into bytes."""
    text = value.strip().upper()
    if not text:
        return 0
    number = text.rstrip("KMGTIB ")
    unit = text[len(number):].strip()
    if unit not in _SIZE_UNITS:
        raise ValueError(f"unrecognized size: {value!r}")
    return int(float(number) * _SIZE_UNITS[unit])


@dataclass
class Config:
    node_id: str = field(default_factory=lambda: os.environ.get("DFS_NODE_ID", "node"))
    cluster_id: str = field(default_factory=lambda: os.environ.get("DFS_CLUSTER_ID", "pool"))
    cluster_secret: str = field(default_factory=lambda: os.environ.get("DFS_CLUSTER_SECRET", ""))
    data_dir: Path = field(default_factory=lambda: Path(os.environ.get("DFS_DATA_DIR", "/data")))
    control_dir: Path = field(default_factory=lambda: Path(os.environ.get("DFS_CONTROL_DIR", "/.dfs")))
    listen_host: str = field(default_factory=lambda: os.environ.get("DFS_LISTEN_HOST", "0.0.0.0"))
    listen_port: int = field(default_factory=lambda: int(os.environ.get("DFS_LISTEN_PORT", "8420")))
    n_copies: int = field(default_factory=lambda: int(os.environ.get("DFS_N_COPIES", "3")))
    write_threshold: int = field(default_factory=lambda: int(os.environ.get("DFS_WRITE_THRESHOLD", "2")))
    # Comma-separated base URLs of known peers, e.g. "http://100.64.0.2:8420,http://100.64.0.3:8420".
    peers: list[str] = field(default_factory=lambda: [
        p.strip() for p in os.environ.get("DFS_PEERS", "").split(",") if p.strip()
    ])
    gossip_interval: float = field(default_factory=lambda: float(os.environ.get("DFS_GOSSIP_INTERVAL", "30")))
    reconcile_interval: float = field(default_factory=lambda: float(os.environ.get("DFS_RECONCILE_INTERVAL", "60")))
    headscale_url: str = field(default_factory=lambda: os.environ.get("DFS_HEADSCALE_URL", ""))
    headscale_authkey: str = field(default_factory=lambda: os.environ.get("DFS_HEADSCALE_AUTHKEY", ""))
    # Cache budget in bytes; 0 or unset = effectively unbounded (design doc §15).
    cache_size: int = field(default_factory=lambda: parse_size(os.environ.get("DFS_CACHE_SIZE", "0")))

    @property
    def meta_dir(self) -> Path:
        return self.control_dir / "meta"

    @property
    def index_path(self) -> Path:
        return self.control_dir / "index.sqlite"

    @property
    def records_log(self) -> Path:
        return self.meta_dir / "records.jsonl"

    @property
    def cache_dir(self) -> Path:
        return self.control_dir / "cache"

    @property
    def tmp_dir(self) -> Path:
        return self.control_dir / "tmp"

    @property
    def peers_path(self) -> Path:
        return self.control_dir / "peers.json"

    @property
    def gossip_cursors_path(self) -> Path:
        return self.control_dir / "gossip_cursors.json"

    @property
    def node_toml_path(self) -> Path:
        return self.control_dir / "node.toml"

    @property
    def pins_path(self) -> Path:
        return self.control_dir / "pins.json"

    @property
    def cache_lru_path(self) -> Path:
        return self.control_dir / "cache_lru.json"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
