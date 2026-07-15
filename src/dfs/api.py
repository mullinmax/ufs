"""Agent HTTP API (design doc §13). MVP subset: health, index read, blob read, locate."""

import shutil
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse

from . import __version__
from .auth import verify
from .config import Config
from .index import Index
from .metalog import MetaLog


def create_app(config: Config, index: Index, metalog: MetaLog) -> FastAPI:
    app = FastAPI(title="dfs-agent", version=__version__)

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
    def get_index():
        # MVP: full dump. Cursor-based deltas (anti-entropy) come with Phase 1.
        records = [
            {
                "path": r.path,
                "version": {"lamport": r.lamport, "node": r.node},
                "state": r.state,
                "hash": r.hash,
                "size": r.size,
                "mtime": r.mtime,
                "holders": index.holders(r.path),
            }
            for r in index.live_records()
        ]
        return {"records": records}

    @app.get("/v1/locate")
    def locate(path: str):
        record = index.get(path)
        if record is None or record.state != "live":
            raise HTTPException(status_code=404, detail="path not found")
        return {"path": path, "holders": index.holders(path)}

    @app.get("/v1/blob/{hash_}")
    def get_blob(hash_: str):
        record = index.by_hash(hash_)
        if record is None:
            raise HTTPException(status_code=404, detail="blob not found")
        file_path = config.data_dir / record.path.lstrip("/")
        resolved = file_path.resolve()
        if not resolved.is_relative_to(config.data_dir.resolve()) or not resolved.is_file():
            raise HTTPException(status_code=404, detail="blob not on disk")
        # FileResponse handles Range requests (range-capable from day one, §7).
        return FileResponse(resolved, media_type="application/octet-stream")

    return app
