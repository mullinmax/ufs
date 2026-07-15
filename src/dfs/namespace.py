"""Union namespace served from the merged index (design doc §12).

The namespace is derived purely from live records in the index — which, after
gossip, is the merged view of every node's holdings. Directories are implicit:
they exist exactly where live file paths imply them. This module is the
backend for FUSE `readdir`/`getattr` and for the `/v1/ls` endpoint.
"""

from typing import Optional

from .index import Index


def _normalize(path: str) -> str:
    path = "/" + path.strip("/")
    return path


def list_dir(index: Index, path: str = "/") -> Optional[list[dict]]:
    """List entries directly under `path` in the union namespace.

    Returns None if `path` is neither the root, an implicit directory, nor
    anything at all. Files return entries with size/mtime; directories with
    just name and type.
    """
    path = _normalize(path)
    prefix = "/" if path == "/" else path + "/"
    files: dict[str, dict] = {}
    dirs: set[str] = set()
    for record in index.live_records():
        if not record.path.startswith(prefix):
            continue
        rest = record.path[len(prefix):]
        name, sep, _ = rest.partition("/")
        if sep:
            dirs.add(name)
        else:
            files[name] = {
                "name": name,
                "type": "file",
                "size": record.size,
                "mtime": record.mtime,
            }
    if not files and not dirs and path != "/":
        return None
    entries = [{"name": d, "type": "dir"} for d in sorted(dirs)]
    entries += [files[n] for n in sorted(files)]
    return entries


def stat_path(index: Index, path: str) -> Optional[dict]:
    """Stat a path in the union namespace: a live file, an implicit dir, or None."""
    path = _normalize(path)
    if path == "/":
        return {"path": "/", "type": "dir"}
    record = index.get(path)
    if record is not None and record.state == "live":
        return {
            "path": path,
            "type": "file",
            "size": record.size,
            "mtime": record.mtime,
            "hash": record.hash,
        }
    prefix = path + "/"
    for record in index.live_records():
        if record.path.startswith(prefix):
            return {"path": path, "type": "dir"}
    return None
