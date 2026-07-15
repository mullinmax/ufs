FROM python:3.12-slim AS base

WORKDIR /app

COPY pyproject.toml VERSION README.md ./
COPY src/ src/
RUN pip install --no-cache-dir .

ENV DFS_DATA_DIR=/data \
    DFS_CONTROL_DIR=/.dfs \
    DFS_LISTEN_PORT=8420

EXPOSE 8420
VOLUME ["/data", "/.dfs"]

ENTRYPOINT ["dfs-agent"]
