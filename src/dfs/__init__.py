"""dfs: pooled multi-node file storage agent."""

from importlib.metadata import PackageNotFoundError, version as _pkg_version
from pathlib import Path

def _read_version() -> str:
    try:
        return _pkg_version("dfs-agent")
    except PackageNotFoundError:
        # Running from a source checkout without installation.
        version_file = Path(__file__).resolve().parents[2] / "VERSION"
        return version_file.read_text().strip()

__version__ = _read_version()
