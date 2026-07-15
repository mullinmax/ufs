"""BLAKE3 file hashing."""

from pathlib import Path

import blake3

_CHUNK = 1024 * 1024


def hash_file(path: Path) -> str:
    """Return the content hash of a file as 'blake3:<hex>'."""
    hasher = blake3.blake3()
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            hasher.update(chunk)
    return f"blake3:{hasher.hexdigest()}"
