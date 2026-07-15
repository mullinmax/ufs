# ufs (dfs)

Universal file storage: pools the storage of several machines into a single
logical drive with configurable redundancy, mountable via FUSE (computers)
and WebDAV (phones), over a self-hosted WireGuard mesh.

- **Design:** [docs/DESIGN.md](docs/DESIGN.md)
- **Roadmap / status:** [ROADMAP.md](ROADMAP.md)

Current state: Phase 1 — multi-node reads. Each agent scans its data
directory, hashes files with BLAKE3, and maintains a rebuildable SQLite index
backed by an append-only JSONL meta log. Nodes discover peers (static
`DFS_PEERS`, the Headscale/tailscale tailnet, and a cached last-known-peers
list), exchange index deltas via anti-entropy gossip
(`GET /v1/index?since=<cursor>` / `POST /v1/index`), and serve the merged
union namespace (`/v1/ls`, `/v1/stat`). Reading a path another node holds
(`/v1/file`) fetches the blob from a holder into `/.dfs/cache`, verifies its
hash, and registers the cached copy as a new holder.

### Two-node quickstart

```bash
# node A
DFS_NODE_ID=alpha DFS_CLUSTER_SECRET=s3cret DFS_LISTEN_PORT=8421 \
  DFS_PEERS=http://host-b:8422 DFS_DATA_DIR=./a/data DFS_CONTROL_DIR=./a/.dfs dfs-agent
# node B
DFS_NODE_ID=beta DFS_CLUSTER_SECRET=s3cret DFS_LISTEN_PORT=8422 \
  DFS_PEERS=http://host-a:8421 DFS_DATA_DIR=./b/data DFS_CONTROL_DIR=./b/.dfs dfs-agent
```

Within a gossip interval (`DFS_GOSSIP_INTERVAL`, default 30s) both nodes see
one merged namespace; `GET /v1/file?path=/...` on either node serves any file
in the pool. Set `DFS_HEADSCALE_URL` + `DFS_HEADSCALE_AUTHKEY` to have the
agent join a Headscale tailnet on startup and discover peers from it
(requires the `tailscale` client on the node).

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
