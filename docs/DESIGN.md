# UFS universal file storage design doc

> Working name is `dfs`. Rename freely; it appears only in the on-disk `.dfs/` directory and the HTTP path prefix, both trivial to change.

Version 0.1 (design draft, pre-implementation)

---

## 1. Purpose

A self-owned application that pools the storage of several machines into a single logical drive, with configurable redundancy, that can be mounted on computers (native filesystem) and phones (WebDAV). Nodes discover each other over a private encrypted mesh, validate membership with a shared cluster secret, and move whole files between each other over HTTP.

The design goal that shaped every decision below: **own it end to end, keep it dead simple, and never depend on a fragile central component.** The index is derived and rebuildable, not a database of record.

---

## 2. Goals and non-goals

### Goals

- One logical namespace ("one big drive") visible on every participating machine.
- Global redundancy target of N copies per file, capacity-permitting.
- Heterogeneous nodes of wildly different sizes (a 4TB laptop alongside 100TB+ servers) coexist, with placement driven by free capacity.
- Passive, redundancy-aware caching: reading a file leaves a copy on the reading node, which counts toward redundancy while present.
- Explicit pinning: a node can pin a path so it always holds a local copy and never evicts it (the GGUF-near-Ollama case).
- Mountable on real computers via FUSE and on phones via WebDAV.
- Survives loss of any single node, and rebuilds its index from what is physically on disk.
- No paid dependency, no external database of record, no single point of failure.

### Non-goals

- Not POSIX-complete. No hardlinks, limited xattrs, no byte-range random rewrites in the MVP (see Consistency).
- Not strongly consistent. Last-writer-wins eventual convergence.
- No sub-file chunking or dedup. Whole-file storage.
- No content version history. A file has exactly one current version; old bytes are not retained.
- Not for a single file larger than the free space of the nodes meant to hold it.
- Not multi-tenant or zero-trust. All nodes sharing the cluster secret are fully trusted peers.
- Not a backup product. N copies gives redundancy, not point-in-time recovery.

---

## 3. Architecture at a glance

Three planes, one agent binary running on every node.

- **Control plane:** membership, the replicated index, redundancy decisions. This is the agent's brain.
- **Data plane:** whole-file transfer between nodes over HTTP, on top of the encrypted mesh.
- **Access plane:** FUSE mount (computers) and WebDAV server (phones) presenting the union namespace.

There is no special "anchor" node role. The always-on servers end up holding nearly everything simply because capacity-based placement favors the nodes with the most free space, and they have the most. This keeps the model uniform: every node runs the same code and differs only in size and uptime.

```
        +-------------------- WireGuard mesh (Headscale) --------------------+
        |                                                                    |
   +---------+        +---------+        +---------+        +---------+
   | node A  | <----> | node B  | <----> | node C  | <----> | phone   |
   | agent   |  HTTP  | agent   |  HTTP  | agent   | WebDAV | (client)|
   | FUSE    |        | FUSE    |        | FUSE    |        |         |
   +---------+        +---------+        +---------+        +---------+
   data + .dfs/meta   data + .dfs/meta   data + .dfs/meta
```

---

## 4. Storage layout on each node

Each node owns a **data directory** plus a small **`.dfs/` control directory**.

```
/data/                        # the logical-path mirror (real folders, real names)
  models/qwen3.gguf
  photos/2026/trip.jpg
  ...
/.dfs/
  meta/                       # durable, human-readable, replicated metadata log
    records.jsonl             # append-only path records (see section 6)
  cache/                      # files fetched from peers but not "owned" placements
  tmp/                        # in-flight writes and fetches
  node.toml                   # this node's identity and config snapshot
  index.sqlite                # materialized global index (rebuildable, disposable)
```

Design choice (Checkpoint 13): **logical-path mirror**, not content-addressed. Files sit at their real paths under `/data`, so a human can browse them and a scan reconstructs the namespace directly. The cost is that identical bytes at two paths are stored twice; accepted for inspectability and simplicity.

The **source of truth for a node** is `/data` plus `/.dfs/meta`. Everything else (`index.sqlite`, `cache/`) is derived and can be deleted and rebuilt.

---

## 5. Network substrate

- **Mesh:** WireGuard via self-hosted **Headscale** (Checkpoint 18). Every node gets a stable encrypted IP; NAT traversal and CGNAT (Starlink) are handled by the mesh with relay fallback.
- **Discovery:** nodes learn peers from the tailnet (Headscale peer list) plus a locally cached last-known-peers list. A node comes up, joins the tailnet, and begins anti-entropy with any reachable peer.
- **Membership auth:** two secrets. The Headscale pre-auth key admits a node to the network. A separate **cluster secret** admits it to *this pool*: every agent-to-agent HTTP request carries a token derived from the cluster secret (HMAC over the request), and requests without it are rejected. The mesh already encrypts transport, so the cluster secret is about pool identity, not wire secrecy.

Operational note: if Headscale is down, existing mesh links keep working and the pool keeps operating. Only *new* nodes joining are blocked until it returns. Run it on one of the always-on servers and back up its small state.

---

## 6. Data model

Everything hangs off a **per-path versioned record**. This is the heart of the system and the reason deletes and re-adds behave correctly.

### Version

A **Lamport counter** plus a node id tiebreak:

```
version = { lamport: <monotonic int>, node: <node-id> }
```

Comparison: higher `lamport` wins; equal `lamport` broken by `node` id. Each node increments its Lamport counter on every local write or delete, and advances it to `max(seen, local) + 1` whenever it receives a higher counter via gossip. This gives a deterministic total order for the common case and lets us detect genuine concurrency (two versions where neither strictly descends from the other).

### Record

The immutable-per-version identity of a path:

```json
{
  "path": "/models/qwen3.gguf",
  "version": { "lamport": 421, "node": "robinton" },
  "state": "live",
  "hash": "blake3:9f2c...",
  "size": 42949672960,
  "mtime": "2026-07-11T14:03:22Z"
}
```

### Holder set

Separate from the record because it changes as copies are made and evicted. Gossiped as its own light stream:

```json
{ "path": "/models/qwen3.gguf", "version": {...}, "holders": ["robinton", "resler", "laptop-1"] }
```

Splitting identity (record) from location (holders) matters: the record for a given version never changes, while holders churn constantly. Merging them would mean re-versioning on every copy, which we do not want.

### The meta log

`/.dfs/meta/records.jsonl` is an append-only log of records and tombstones this node knows about, one JSON object per line, human-readable. It is the durable, replicated metadata. `index.sqlite` is built from merging every node's meta log and exists only for fast queries; losing it triggers a rebuild, not a crisis.

**What "rebuild from disk" recovers, precisely.** Data files plus meta log recover the full live set, versions, and tombstones. If the meta log is also lost on a node, rescanning `/data` recovers that node's live files (path, hash, size) but not versions or tombstones; those come back by re-gossiping with peers. Only a simultaneous total loss of every meta copy across the whole cluster loses versions and deletion history, and at that point the live set is still reconstructed from raw bytes. History loss on catastrophic rebuild is accepted.

---

## 7. Consistency and write semantics

### No isolated edits

A create, modify, or delete is accepted only if the writing node can reach at least one peer and meet the write threshold. If it cannot, the operation raises and the FUSE layer returns an I/O error (`EROFS`) to the application. This eliminates the main conflict source: a lone node editing in isolation and clashing on return.

Reads are never blocked this way. An isolated node still serves any file it physically holds.

### Write threshold

Default: a write must be durably committed on **at least 2 distinct nodes** before it returns success. The reconciler later tops the copy count up to N in the background. This means writes succeed whenever the two big servers are up (nearly always) while guaranteeing no write ever lives on a single node. Configurable up to strict-N (all N copies synchronous before success) at the cost of writes failing when fewer than N nodes are online.

### Write path

1. Application writes to the FUSE mount on node A. Bytes buffer to `/.dfs/tmp`.
2. On `release`/`fsync`, node A checks reachability. If it cannot reach a peer, raise `EROFS`.
3. Node A computes the BLAKE3 hash, assigns a new version (`lamport++`), moves the file into `/data` at its logical path, writes the record to the meta log.
4. Node A pushes the file to at least one other reachable node (2 holders total), synchronously.
5. Once the second holder confirms, the write returns success. Record and holder update gossip out.
6. The reconciler asynchronously raises the copy count to N.

### Read path (MVP: fetch-then-open)

1. Application opens `/foo` on node B.
2. If node B holds `/foo` locally and the hash matches the current version, serve from disk.
3. Otherwise look up holders in the index, pick a reachable one, fetch the whole file over HTTP into `/.dfs/cache`, then serve.
4. If no holder is reachable, return `EIO`.

Later refinement (Checkpoint 14, "simple first, graceful later"): stream byte ranges on demand instead of prefetching the whole file, so opening a 40GB GGUF returns the first byte quickly. The HTTP blob endpoint is range-capable from day one to make this a drop-in upgrade.

### Conflicts

With no-isolated-edits, genuine conflicts are rare: they require two connected partitions that each independently meet the write threshold and write the same path, then merge. Detection: two versions with concurrent Lamport counters (neither descends from the other). Resolution: LWW picks the deterministic winner by `(lamport, node)`; the loser is preserved as a conflict copy at `/foo.conflict-<node>-<lamport>` and surfaced. Cheap safety net, seldom triggered.

Future option (not built now): full version vectors for provably correct concurrency detection. Lamport plus node id is sufficient for home use.

---

## 8. Redundancy and placement

- **Redundancy target:** a single global `N` (Checkpoint 11: no per-folder levels). Default N = 3 suggested; configurable.
- **Placement:** capacity-based, no anchor role (Checkpoint 17). When a file needs another holder, the target is the reachable node with the most free space that does not already hold it. Because the big servers have the most free space, they naturally accumulate near-complete copies without any special-casing.
- **Reconciler loop:** periodically scans the index for live paths whose reachable holder count is below N, and for each, picks a source holder and a target node and pushes a copy, then updates holders and gossips. It also handles over-replication cleanup only through the eviction policy (below), never by force-deleting a designated copy.

Under-replication from a node going offline self-heals: its holdings drop below N among reachable nodes, the reconciler notices, and it re-replicates elsewhere.

---

## 9. Cache and eviction

Every node caches what it reads into `/.dfs/cache`. Cached copies count toward N while present (Checkpoint 10).

**Redundancy-aware LRU eviction.** When a node needs space, it evicts the least-recently-used cached file, subject to one hard rule: **never evict a file if doing so would drop its global holder count below N.** Pinned files are never evicted. This makes the cache double as opportunistic redundancy exactly as intended: files that are scarce cluster-wide stick around on whoever holds them, and only safely-replicated files get reclaimed.

Distinction between `cache/` and owned placements: a reconciler-assigned placement is a deliberate copy that counts as a "real" holder; a cache entry is opportunistic. Both count toward N for read-availability, but the eviction rule protects any copy that is load-bearing for the N target regardless of which bucket it is in.

---

## 10. Deletion and tombstones

- A delete writes a **tombstone**: a record with `state: "tombstone"` at a new version (`lamport++`), then removes the local bytes and gossips the tombstone.
- On receiving a tombstone whose version is greater than their held version, other holders remove their bytes and record the tombstone.
- **Tombstones are kept forever.** They are tiny (roughly 150 to 300 bytes: path, version, hash, timestamp). A million lifetime deletes is a few hundred MB replicated, which any node shrugs off. This is what lets a straggler that returns after months learn that a path was deleted and purge its stale copy. (An optional long TTL is possible for churny delete-heavy workloads, with the known risk that a node offline longer than the TTL resurrects stale files. Defaulted off.)

### Two cases this design handles correctly

- **Delete then identical re-add.** The delete is version v+1 (tombstone). The re-add is version v+2 (live), regardless of byte-identical content. A tombstone only suppresses versions at or below its own, so the re-added file is live and never ghost-deleted. Content is irrelevant; the version decides.
- **Indefinite node absence.** We never try to prove a file left every node. The tombstone persists, so whenever the straggler returns it is told to purge anything it holds at or below the tombstone's version.

### Straggler reconciliation

On rejoin a node runs anti-entropy and, for each path it holds, compares its local version to the cluster version: a higher-versioned tombstone means purge; a higher-versioned live record means its copy is stale, so drop it and let the reconciler decide whether to re-fetch. Because isolated edits are forbidden, a straggler brings no conflicting writes, only stale reads to reconcile.

---

## 11. Pinning (the GGUF-near-Ollama case)

A node can pin a path or prefix in its `node.toml`:

```toml
[[pin]]
prefix = "/models/"
```

Pinned paths are always fetched to the local node (proactively, like a warmup) and never evicted. This guarantees the model sits on the same box as the Ollama container for fast boots, and it is the locality requirement expressed as cache policy rather than as forced placement.

Companion query for "which node already has this model so I can spin it up there": `GET /v1/locate?path=/models/foo.gguf` returns the current holder set from the index. Scheduling logic can read that and start the workload where the file already lives.

---

## 12. Access layer

### FUSE (computers, Checkpoint 15: full participant everywhere)

Every node that can run FUSE presents the union namespace as a real mount. Operations to implement: `lookup`, `getattr`, `readdir`, `open`, `read`, `write`, `create`, `unlink`, `rename`, `release`, `flush`, `fsync`, `truncate`. The namespace is served from the index; file bytes are served from local disk or fetched on open per the read path. Writes buffer to `/.dfs/tmp` and commit through the write path on `release`/`fsync`.

### WebDAV (phones)

Phones cannot run FUSE (no FUSE on iOS, root-only on Android), so any always-on node also runs a WebDAV server exposing the same union namespace. iOS Files and Android file managers mount WebDAV natively. Same backend, second front door.

---

## 13. HTTP API (agent to agent, and local control)

All agent-to-agent calls carry the cluster HMAC token and run over the mesh.

| Method | Path | Purpose |
|---|---|---|
| GET | `/v1/health` | liveness and free-capacity report |
| GET | `/v1/index?since=<cursor>` | anti-entropy: return record and holder deltas |
| POST | `/v1/index` | receive gossiped records, holders, tombstones |
| GET | `/v1/blob/{hash}` | fetch file bytes, range-capable |
| POST | `/v1/blob` | receive a pushed copy (write replication or reconciler) |
| GET | `/v1/locate?path=` | return current holders of a path |
| POST | `/v1/pin` / `DELETE /v1/pin` | manage local pins |

Gossip is anti-entropy: nodes periodically exchange deltas since a per-peer cursor and merge by version. No consensus, no quorum.

---

## 14. Failure and recovery summary

| Failure | Recovery |
|---|---|
| A node goes offline | Reconciler sees under-replication among reachable nodes, re-replicates to meet N |
| `index.sqlite` lost on a node | Rebuild by merging local meta log with peer anti-entropy |
| `.dfs/meta` lost, `/data` intact | Rescan `/data` for live files; versions and tombstones restored from peers |
| Total cluster meta loss | Walk `/data` on all nodes, reconstruct live set; deletion history and versions lost (accepted) |
| Headscale down | Pool keeps running on existing links; only new node joins are blocked |
| Write attempted while isolated | `EROFS` to the application (no-isolated-edits) |

---

## 15. Deployment

Every node runs the agent as a Docker container. FUSE inside a container needs:

```
docker run \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --security-opt apparmor:unconfined \
  -v /srv/dfs/data:/data \
  -v /srv/dfs/meta:/.dfs/meta \
  -v /mnt/pool:/mnt/pool:rshared \
  --env-file dfs.env \
  dfs-agent:latest
```

The host mount point needs shared propagation (`rshared`) or the mount stays trapped inside the container.

### Configuration (env vars)

```
DFS_NODE_ID=robinton
DFS_CLUSTER_ID=home-pool
DFS_CLUSTER_SECRET=...            # pool membership / HMAC
DFS_HEADSCALE_URL=https://...
DFS_HEADSCALE_AUTHKEY=...
DFS_N_COPIES=3
DFS_WRITE_THRESHOLD=2
DFS_DATA_DIR=/data
DFS_CACHE_SIZE=4TB               # 0 or unset on huge servers = effectively unbounded
DFS_MOUNTPOINT=/mnt/pool
DFS_WEBDAV_ENABLE=true           # on always-on nodes
```

---

## 16. Technology stack

| Concern | Choice | Notes |
|---|---|---|
| Agent runtime | Python, FastAPI, asyncio, httpx | Network- and disk-bound before code-bound |
| FUSE binding | pyfuse3 | asyncio-friendly; fusepy is the fallback |
| Hashing | BLAKE3 | fast, content addressing for integrity checks |
| Materialized index | SQLite (`index.sqlite`) | safe because it is rebuildable |
| Durable metadata | append-only JSONL (`.dfs/meta`) | human-readable source of truth |
| Mesh | Headscale + WireGuard | self-hosted, CGNAT-tolerant |
| Phone access | WebDAV (wsgidav or built-in) | native mount on iOS and Android |
| Packaging | Docker | one image, runs everywhere |

### Prior art worth mining (not adopting)

- **git-annex:** numcopies and location-tracking model.
- **Syncthing:** device discovery, peer handshake, block-exchange protocol.

---

## 17. Build phases

- **Phase 0 - single node.** Data folder, scanner, BLAKE3 hasher, SQLite index, read-only FUSE mount showing local files.
- **Phase 1 - two nodes, reads.** Headscale mesh, cluster-secret auth, anti-entropy gossip, `/v1/blob` fetch, fetch-then-open reads, union namespace.
- **Phase 2 - writes.** Write path with threshold 2, no-isolated-edits guard (`EROFS`), reconciler to N copies, capacity-based placement.
- **Phase 3 - deletes.** Lamport versioning, tombstones, straggler reconciliation, delete-then-readd correctness.
- **Phase 4 - cache and pins.** Redundancy-aware LRU eviction, pinning, `/v1/locate`.
- **Phase 5 - phones.** WebDAV gateway on always-on nodes.
- **Phase 6 - refinements.** Streaming byte-range reads, conflict-copy surfacing, metrics, scheduling helper.

---

## 18. Open questions for later

- Exact anti-entropy cursor format and gossip cadence (per-peer version vector vs simple high-water Lamport per node).
- Rename handling: cheap as a metadata operation in the logical-path mirror, but confirm behavior under concurrent rename plus edit.
- Large-directory `readdir` performance from the index at scale.
- Whether to add optional per-path pin replication (pin followed cluster-wide, not just locally).
- Metrics surface: free capacity, under-replicated file count, cache hit rate, replication lag.
