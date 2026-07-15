"""Append-only JSONL metadata log — the durable source of truth (design doc §6)."""

from pathlib import Path
from typing import Iterator

from .index import Record


class MetaLog:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: Record) -> None:
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(record.to_json() + "\n")

    def read_all(self) -> Iterator[Record]:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield Record.from_json(line)
