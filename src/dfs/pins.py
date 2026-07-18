"""Pinning (design doc §11): a node pins a path prefix so it always holds a
local copy (proactive warmup) and never evicts it.

Pins come from two places:
- `node.toml` in the control dir (`[[pin]] prefix = "/models/"`) — the
  operator-managed set, read at startup and re-read on demand.
- The pin API (`POST /v1/pin` / `DELETE /v1/pin`) — the dynamic set,
  persisted to `pins.json` in the control dir so it survives restarts.

Matching is a literal prefix match on the logical path, with the exact path
itself also matching (pin "/models" covers "/models" and "/models/foo.gguf").
"""

import json
import tomllib
from pathlib import Path

from .config import Config


class PinConflictError(Exception):
    """The pin is managed by node.toml and cannot be removed via the API."""


class PinStore:
    def __init__(self, config: Config):
        self.config = config
        self._toml_pins = self._load_toml()
        self._api_pins = self._load_api_pins()

    def _load_toml(self) -> list[str]:
        path = self.config.node_toml_path
        if not path.is_file():
            return []
        with path.open("rb") as fh:
            doc = tomllib.load(fh)
        return [_normalize(entry["prefix"]) for entry in doc.get("pin", [])
                if entry.get("prefix")]

    def _load_api_pins(self) -> list[str]:
        path = self.config.pins_path
        if not path.is_file():
            return []
        return [_normalize(p) for p in json.loads(path.read_text())]

    def _save_api_pins(self) -> None:
        self.config.pins_path.write_text(json.dumps(sorted(self._api_pins)))

    def prefixes(self) -> list[dict]:
        return ([{"prefix": p, "source": "node.toml"} for p in self._toml_pins]
                + [{"prefix": p, "source": "api"} for p in sorted(self._api_pins)])

    def is_pinned(self, path: str) -> bool:
        for prefix in self._toml_pins + self._api_pins:
            if path == prefix.rstrip("/") or path.startswith(
                    prefix if prefix.endswith("/") else prefix + "/"):
                return True
        return False

    def add(self, prefix: str) -> bool:
        """Add a dynamic pin. Returns False if it was already pinned."""
        prefix = _normalize(prefix)
        if prefix in self._toml_pins or prefix in self._api_pins:
            return False
        self._api_pins.append(prefix)
        self._save_api_pins()
        return True

    def remove(self, prefix: str) -> None:
        """Remove a dynamic pin. Raises KeyError if unknown, PinConflictError
        if the pin is operator-managed in node.toml."""
        prefix = _normalize(prefix)
        if prefix in self._api_pins:
            self._api_pins.remove(prefix)
            self._save_api_pins()
            return
        if prefix in self._toml_pins:
            raise PinConflictError(prefix)
        raise KeyError(prefix)


def _normalize(prefix: str) -> str:
    if not prefix.startswith("/") or ".." in prefix.split("/"):
        raise ValueError(f"invalid pin prefix: {prefix!r}")
    return prefix
