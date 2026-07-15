"""Agent configuration, read from environment variables (see design doc §15)."""

import os
from dataclasses import dataclass, field
from pathlib import Path


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
    headscale_url: str = field(default_factory=lambda: os.environ.get("DFS_HEADSCALE_URL", ""))
    headscale_authkey: str = field(default_factory=lambda: os.environ.get("DFS_HEADSCALE_AUTHKEY", ""))

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

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
