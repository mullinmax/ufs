# UFS / dfs — Implementation Plan & Roadmap

This tracks implementation of the [design doc](docs/DESIGN.md) against its build phases (§17).
Checked items are complete; unchecked items are the plan for future passes.

## Versioning & CI (this pass)

- [x] Single source of truth for the version number: the top-level `VERSION` file. `pyproject.toml` reads it dynamically, `dfs.__version__` derives from package metadata (or the file in a source checkout), and CI reads it for Docker tags. Bump the version by editing `VERSION` only.
- [x] GitHub Actions: run tests on PRs and main (`ci.yml`)
- [x] GitHub Actions: build & push Docker image on PR open/update → `ghcr.io/<repo>:pr-<n>` (`docker.yml`)
- [x] GitHub Actions: push to main → `beta` (and `beta-<version>`) tags
- [x] GitHub Actions: published release → `latest` and `<version>` tags

## Phase 0 — single node (this pass, MVP)

- [x] Project skeleton (`src/dfs/`), config from `DFS_*` env vars
- [x] Data-directory scanner (logical-path mirror under `/data`)
- [x] BLAKE3 file hashing
- [x] SQLite materialized index (derived, disposable, rebuildable)
- [x] Append-only JSONL meta log (`.dfs/meta/records.jsonl`) as durable metadata
- [x] Per-path versioned records (Lamport counter + node id, LWW merge)
- [x] Holder-set tracking in the index
- [x] Agent HTTP API: `GET /v1/health`, `GET /v1/index` (full dump), `GET /v1/blob/{hash}` (range-capable), `GET /v1/locate`
- [x] Cluster-secret HMAC auth on agent endpoints
- [x] Dockerfile (one image, runs everywhere)
- [x] Unit tests (scanner, index LWW, meta log, hashing, auth, API)
- [ ] Read-only FUSE mount showing local files (pyfuse3)

## Phase 1 — two nodes, reads (this pass)

- [x] Headscale/WireGuard mesh join + peer discovery (`mesh.py`: best-effort `tailscale up` against `DFS_HEADSCALE_URL`, tailnet peer list + `DFS_PEERS` + cached last-known-peers in `peers.py`)
- [x] Anti-entropy gossip: `GET /v1/index?since=<cursor>` deltas, `POST /v1/index` merge (`gossip.py`; per-peer pull/push cursors over local index sequence numbers, remote-won records appended to the local meta log)
- [x] Fetch-then-open reads across nodes into `/.dfs/cache` (`fetch.py`: local → cache → holder fetch with BLAKE3 verification; cached copies register as holders and count toward N)
- [x] Union namespace served from the merged index (`namespace.py`; exposed as `GET /v1/ls`, `GET /v1/stat`, and `GET /v1/file` until the FUSE layer lands)

## Phase 2 — writes (this pass)

- [x] Write path: buffer to `/.dfs/tmp`, hash, version, commit to `/data` + meta log (`writer.py`; exposed as `PUT /v1/file` until the FUSE layer lands)
- [x] Write threshold (default 2 synchronous holders), `POST /v1/blob` push
- [x] No-isolated-edits guard (`EROFS` when no peer reachable; HTTP 503 on `PUT /v1/file`)
- [x] Reconciler loop: top up copies to N, capacity-based placement (`reconcile.py`: pushes local copies to the reachable non-holder with the most free space)
- [ ] FUSE write operations (`create`, `write`, `rename`, `truncate`, `fsync`) — blocked on the read-only FUSE mount (Phase 0 leftover)

## Phase 3 — deletes (this pass)

- [x] Tombstone records on delete (kept forever) (`delete.py`: `DELETE /v1/file` writes a `state: "tombstone"` record at `lamport++`, appends it to the meta log, purges local bytes; same no-isolated-edits guard as writes, plus eager best-effort tombstone push to reachable peers)
- [x] Tombstone propagation: higher-versioned tombstone purges local bytes (`apply_remote_record`, applied on every gossip merge; stale holder gossip for tombstoned paths is skipped)
- [x] Delete-then-identical-re-add correctness (re-add is a new higher version; a tombstone only suppresses versions at or below its own — content irrelevant)
- [x] Straggler reconciliation on rejoin (rescan re-registers stale files at their old version from the meta log; the gossiped tombstone or newer live record then purges/drops the stale copy and holder claim)

## Phase 4 — cache and pins

- [ ] Redundancy-aware LRU eviction (never drop below N global holders)
- [ ] Pinning via `node.toml` (`[[pin]] prefix = ...`), proactive warmup, never evict
- [ ] `POST /v1/pin` / `DELETE /v1/pin` endpoints
- [ ] `DFS_CACHE_SIZE` enforcement

## Phase 5 — phones

- [ ] WebDAV gateway on always-on nodes (same union namespace)

## Phase 6 — refinements

- [ ] Streaming byte-range reads on open (drop-in over the range-capable blob endpoint)
- [ ] Conflict-copy surfacing (`/foo.conflict-<node>-<lamport>`) and UX
- [ ] Metrics: free capacity, under-replicated count, cache hit rate, replication lag
- [ ] Scheduling helper: start workloads where the file already lives (`/v1/locate` consumers)

## Open questions (design doc §18)

- [ ] Anti-entropy cursor format and gossip cadence
- [ ] Concurrent rename + edit semantics
- [ ] Large-directory `readdir` performance
- [ ] Optional cluster-wide pin replication
