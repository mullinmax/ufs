"""Agent HTTP API (design doc §13).

Phase 0: health, index read, blob read, locate.
Phase 1: anti-entropy deltas (`GET /v1/index?since=`), gossip merge
(`POST /v1/index`), union-namespace listing (`GET /v1/ls`), and cross-node
fetch-then-open reads (`GET /v1/file`).
Phase 2: writes (`PUT /v1/file`) and replica pushes (`POST /v1/blob`).
Phase 3: deletes (`DELETE /v1/file` writes a tombstone; gossiped tombstones
purge bytes on every holder).
Phase 4: pins (`GET/POST/DELETE /v1/pin`; pinned prefixes are warmed up and
never evicted by the cache loop).
"""

import asyncio
import binascii
import json
import shutil
import uuid

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from . import __version__
from .auth import verify
from .config import Config
from .delete import Deleter
from .fetch import Fetcher
from .gossip import merge_delta
from .index import Index
from .metalog import MetaLog
from .pins import PinConflictError, PinStore
from .writer import (
    IsolatedWriteError,
    WriteThresholdError,
    Writer,
    decode_record_header,
    receive_blob,
)
from . import namespace


class IndexDelta(BaseModel):
    node: str | None = None
    records: list[dict] = []
    holders: list[dict] = []


def create_app(
    config: Config,
    index: Index,
    metalog: MetaLog,
    fetcher: Fetcher | None = None,
    writer: Writer | None = None,
    deleter: Deleter | None = None,
    pins: PinStore | None = None,
    lifespan=None,
) -> FastAPI:
    app = FastAPI(title="dfs-agent", version=__version__, lifespan=lifespan)

    @app.middleware("http")
    async def cluster_auth(request: Request, call_next):
        token = request.headers.get("x-dfs-token", "")
        if not verify(config.cluster_secret, request.method, request.url.path, token):
            return JSONResponse({"detail": "invalid cluster token"}, status_code=403)
        return await call_next(request)

    @app.get("/v1/health")
    def health():
        usage = shutil.disk_usage(config.data_dir)
        return {
            "node": config.node_id,
            "cluster": config.cluster_id,
            "version": __version__,
            "free_bytes": usage.free,
            "total_bytes": usage.total,
        }

    @app.get("/v1/index")
    def get_index(since: int = 0):
        # Anti-entropy: everything stamped after the caller's cursor.
        # since=0 (the default) is a full dump.
        records, holders, cursor = index.changes_since(since)
        return {
            "node": config.node_id,
            "cursor": cursor,
            "records": [r.to_dict() for r in records],
            "holders": holders,
        }

    @app.post("/v1/index")
    def post_index(delta: IndexDelta):
        accepted = merge_delta(config, index, metalog, delta.records, delta.holders)
        return {"accepted": accepted}

    @app.get("/v1/locate")
    def locate(path: str):
        record = index.get(path)
        if record is None or record.state != "live":
            raise HTTPException(status_code=404, detail="path not found")
        return {"path": path, "holders": index.holders(path)}

    @app.get("/v1/ls")
    def ls(path: str = "/"):
        entries = namespace.list_dir(index, path)
        if entries is None:
            raise HTTPException(status_code=404, detail="no such directory")
        return {"path": path, "entries": entries}

    @app.get("/v1/stat")
    def stat(path: str):
        info = namespace.stat_path(index, path)
        if info is None:
            raise HTTPException(status_code=404, detail="path not found")
        return info

    @app.get("/v1/file")
    def get_file(path: str):
        # Fetch-then-open: serve local bytes, or pull them from a holder first.
        if fetcher is None:
            raise HTTPException(status_code=503, detail="fetcher not configured")
        try:
            local = fetcher.open_path(path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="path not found")
        except IOError as exc:
            raise HTTPException(status_code=502, detail=str(exc))
        return FileResponse(local, media_type="application/octet-stream")

    async def _buffer_body(request: Request, prefix: str):
        buffered = config.tmp_dir / f"{prefix}-{uuid.uuid4().hex}"
        buffered.parent.mkdir(parents=True, exist_ok=True)
        with buffered.open("wb") as fh:
            async for chunk in request.stream():
                fh.write(chunk)
        return buffered

    @app.put("/v1/file")
    async def put_file(path: str, request: Request):
        # Write path (§7): buffer, then commit + replicate to the threshold.
        if writer is None:
            raise HTTPException(status_code=503, detail="writer not configured")
        if not path.startswith("/") or ".." in path.split("/"):
            raise HTTPException(status_code=400, detail="invalid path")
        buffered = await _buffer_body(request, "write")
        try:
            record = await asyncio.to_thread(writer.write, path, buffered)
        except IsolatedWriteError:
            # No-isolated-edits guard: the FUSE layer maps this to EROFS.
            raise HTTPException(status_code=503, detail="read-only: no peer reachable")
        except WriteThresholdError as exc:
            raise HTTPException(status_code=502, detail=str(exc))
        finally:
            buffered.unlink(missing_ok=True)
        return {"record": record.to_dict(), "holders": index.holders(path)}

    @app.delete("/v1/file")
    async def delete_file(path: str):
        # Delete path (§10): tombstone, purge local bytes, notify peers.
        if deleter is None:
            raise HTTPException(status_code=503, detail="deleter not configured")
        try:
            tombstone = await asyncio.to_thread(deleter.delete, path)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="path not found")
        except IsolatedWriteError:
            raise HTTPException(status_code=503, detail="read-only: no peer reachable")
        except WriteThresholdError as exc:
            raise HTTPException(status_code=502, detail=str(exc))
        return {"record": tombstone.to_dict()}

    @app.get("/v1/pin")
    def list_pins():
        if pins is None:
            raise HTTPException(status_code=503, detail="pins not configured")
        return {"pins": pins.prefixes()}

    @app.post("/v1/pin")
    def add_pin(prefix: str):
        # Pin a prefix on this node (§11); the cache loop warms it up next round.
        if pins is None:
            raise HTTPException(status_code=503, detail="pins not configured")
        try:
            added = pins.add(prefix)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"added": added, "pins": pins.prefixes()}

    @app.delete("/v1/pin")
    def remove_pin(prefix: str):
        if pins is None:
            raise HTTPException(status_code=503, detail="pins not configured")
        try:
            pins.remove(prefix)
        except KeyError:
            raise HTTPException(status_code=404, detail="prefix not pinned")
        except PinConflictError:
            raise HTTPException(
                status_code=409, detail="pin is managed by node.toml; edit that file")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"pins": pins.prefixes()}

    @app.post("/v1/blob")
    async def post_blob(request: Request):
        # Replication receive: write path pushes and reconciler top-ups.
        header = request.headers.get("x-dfs-record", "")
        try:
            record = decode_record_header(header)
        except (binascii.Error, json.JSONDecodeError, KeyError, UnicodeDecodeError):
            raise HTTPException(status_code=400, detail="missing or malformed x-dfs-record")
        buffered = await _buffer_body(request, "recv")
        try:
            await asyncio.to_thread(receive_blob, config, index, metalog, record, buffered)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"stored": True, "node": config.node_id}

    @app.get("/v1/blob/{hash_}")
    def get_blob(hash_: str):
        record = index.by_hash(hash_)
        if record is None:
            raise HTTPException(status_code=404, detail="blob not found")
        for base in (config.data_dir, config.cache_dir):
            file_path = (base / record.path.lstrip("/")).resolve()
            if file_path.is_relative_to(base.resolve()) and file_path.is_file():
                # FileResponse handles Range requests (range-capable from day one, §7).
                return FileResponse(file_path, media_type="application/octet-stream")
        raise HTTPException(status_code=404, detail="blob not on disk")

    return app
