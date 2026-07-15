# ufs (dfs)

Universal file storage: pools the storage of several machines into a single
logical drive with configurable redundancy, mountable via FUSE (computers)
and WebDAV (phones), over a self-hosted WireGuard mesh.

- **Design:** [docs/DESIGN.md](docs/DESIGN.md)
- **Roadmap / status:** [ROADMAP.md](ROADMAP.md)

Current state: Phase 0 MVP — a single-node agent that scans its data
directory, hashes files with BLAKE3, maintains a rebuildable SQLite index
backed by an append-only JSONL meta log, and serves the agent HTTP API
(`/v1/health`, `/v1/index`, `/v1/blob/{hash}`, `/v1/locate`).

## Run

```bash
pip install -e .[dev]
pytest                                   # tests
DFS_NODE_ID=mynode DFS_DATA_DIR=./data DFS_CONTROL_DIR=./.dfs dfs-agent
```

Or with Docker:

```bash
docker run -p 8420:8420 \
  -v /srv/dfs/data:/data -v /srv/dfs/dfs:/.dfs \
  -e DFS_NODE_ID=mynode -e DFS_CLUSTER_SECRET=... \
  ghcr.io/mullinmax/ufs:beta
```

## Versioning

The version number has a single source of truth: the top-level [`VERSION`](VERSION)
file. `pyproject.toml`, `dfs.__version__`, and the Docker image tags in CI all
derive from it. To cut a new version, edit `VERSION` only.

Image tags: PRs build `pr-<n>`, pushes to `main` build `beta`, published
releases build `latest` + `<version>`.
