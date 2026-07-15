"""Agent entrypoint: scan the data dir, build the index, join the mesh,
gossip with peers, serve the HTTP API."""

import asyncio
import contextlib
import logging

import uvicorn

from . import __version__
from .api import create_app
from .config import Config
from .fetch import Fetcher
from .gossip import Gossip
from .index import Index
from .mesh import join_mesh, discover_peers
from .metalog import MetaLog
from .peers import PeerStore
from .scanner import scan

log = logging.getLogger("dfs")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    config = Config()
    config.ensure_dirs()
    log.info("dfs-agent %s starting as node %s", __version__, config.node_id)

    index = Index(config.index_path)
    metalog = MetaLog(config.records_log)
    indexed = scan(config, index, metalog)
    log.info("scan complete: %d files indexed", indexed)

    peers = PeerStore(config.peers_path, static_peers=config.peers)
    join_mesh(config)
    discover_peers(config, peers)
    log.info("known peers: %s", ", ".join(peers.urls()) or "(none)")

    fetcher = Fetcher(config, index, peers)
    gossip = Gossip(config, index, metalog, peers)

    @contextlib.asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(gossip.run())
        try:
            yield
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = create_app(config, index, metalog, fetcher=fetcher, lifespan=lifespan)
    uvicorn.run(app, host=config.listen_host, port=config.listen_port)


if __name__ == "__main__":
    main()
