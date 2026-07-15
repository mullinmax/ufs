"""Agent entrypoint: scan the data dir, build the index, serve the HTTP API."""

import logging

import uvicorn

from . import __version__
from .api import create_app
from .config import Config
from .index import Index
from .metalog import MetaLog
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

    app = create_app(config, index, metalog)
    uvicorn.run(app, host=config.listen_host, port=config.listen_port)


if __name__ == "__main__":
    main()
