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

    @property
    def meta_dir(self) -> Path:
        return self.control_dir / "meta"

    @property
    def index_path(self) -> Path:
        return self.control_dir / "index.sqlite"

    @property
    def records_log(self) -> Path:
        return self.meta_dir / "records.jsonl"

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        (self.control_dir / "cache").mkdir(parents=True, exist_ok=True)
        (self.control_dir / "tmp").mkdir(parents=True, exist_ok=True)
