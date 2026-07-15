"""Agent HTTP API (design doc §13).

Phase 0: health, index read, blob read, locate.
Phase 1: anti-entropy deltas (`GET /v1/index?since=`), gossip merge
(`POST /v1/index`), union-namespace listing (`GET /v1/ls`), and cross-node
fetch-then-open reads (`GET /v1/file`).
"""

import shutil

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from . import __version__
from .auth import verify
from .config import Config
from .fetch import Fetcher
from .gossip import merge_delta
from .index import Index
from .metalog import MetaLog
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
        accepted = merge_delta(index, metalog, delta.records, delta.holders)
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
