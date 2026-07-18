"""Phase 1: anti-entropy gossip, fetch-then-open reads, union namespace, peers."""

import asyncio
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from dfs.api import create_app
from dfs.config import Config
from dfs.fetch import Fetcher
from dfs.gossip import Gossip, merge_delta
from dfs.index import Index, Record
from dfs.metalog import MetaLog
from dfs.namespace import list_dir, stat_path
from dfs.peers import PeerStore
from dfs.scanner import scan


def make_node(tmp_path: Path, node_id: str) -> tuple[Config, Index, MetaLog]:
    cfg = Config()
    cfg.node_id = node_id
    cfg.data_dir = tmp_path / node_id / "data"
    cfg.control_dir = tmp_path / node_id / ".dfs"
    cfg.ensure_dirs()
    return cfg, Index(cfg.index_path), MetaLog(cfg.records_log)


def test_index_changes_since(tmp_path: Path):
    index = Index(tmp_path / "idx.sqlite")
    index.upsert(Record(path="/a", lamport=1, node="n", state="live", hash="blake3:a", size=1))
    index.set_holder("/a", "n")
    records, holders, cursor = index.changes_since(0)
    assert [r.path for r in records] == ["/a"]
    assert holders == [{"path": "/a", "node": "n"}]
    assert cursor == 2

    # Nothing new after the cursor.
    records, holders, new_cursor = index.changes_since(cursor)
    assert records == [] and holders == [] and new_cursor == cursor

    # A losing LWW merge and a repeated holder do not advance the sequence.
    assert not index.upsert(Record(path="/a", lamport=0, node="m", state="live"))
    assert not index.set_holder("/a", "n")
    assert index.cursor() == cursor

    index.upsert(Record(path="/b", lamport=2, node="n", state="live", hash="blake3:b", size=2))
    records, _, _ = index.changes_since(cursor)
    assert [r.path for r in records] == ["/b"]


def test_index_migrates_pre_seq_schema(tmp_path: Path):
    import sqlite3

    db = tmp_path / "idx.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE records (path TEXT PRIMARY KEY, lamport INTEGER NOT NULL,"
                 " node TEXT NOT NULL, state TEXT NOT NULL, hash TEXT, size INTEGER, mtime TEXT)")
    conn.execute("CREATE TABLE holders (path TEXT NOT NULL, node TEXT NOT NULL, PRIMARY KEY (path, node))")
    conn.commit()
    conn.close()

    index = Index(db)  # must drop and recreate the old tables
    assert index.upsert(Record(path="/x", lamport=1, node="n", state="live"))
    assert index.get("/x") is not None


def test_merge_delta_appends_winners_to_metalog(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    rec = Record(path="/f", lamport=3, node="b", state="live", hash="blake3:f", size=1)
    accepted = merge_delta(cfg, index, metalog, [rec.to_dict()], [{"path": "/f", "node": "b"}])
    assert accepted == 1
    assert index.holders("/f") == ["b"]
    assert list(metalog.read_all()) == [rec]
    # Re-merging the same delta is a no-op (no duplicate metalog lines).
    assert merge_delta(cfg, index, metalog, [rec.to_dict()], []) == 0
    assert len(list(metalog.read_all())) == 1


def test_index_endpoints_delta_roundtrip(tmp_path: Path):
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a")
    (cfg_a.data_dir / "f.txt").write_bytes(b"hello")
    scan(cfg_a, index_a, metalog_a)

    cfg_b, index_b, metalog_b = make_node(tmp_path, "b")
    client_a = TestClient(create_app(cfg_a, index_a, metalog_a))
    client_b = TestClient(create_app(cfg_b, index_b, metalog_b))

    delta = client_a.get("/v1/index", params={"since": 0}).json()
    assert delta["node"] == "a" and delta["cursor"] > 0

    resp = client_b.post("/v1/index", json={"node": "a", "records": delta["records"],
                                            "holders": delta["holders"]})
    assert resp.json()["accepted"] == 1
    assert index_b.get("/f.txt").state == "live"
    assert index_b.holders("/f.txt") == ["a"]

    # b now knows the file even though it never held the bytes.
    assert client_b.get("/v1/locate", params={"path": "/f.txt"}).json()["holders"] == ["a"]


def test_gossip_sync_converges(tmp_path: Path):
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a")
    (cfg_a.data_dir / "only-on-a.txt").write_bytes(b"aaa")
    scan(cfg_a, index_a, metalog_a)

    cfg_b, index_b, metalog_b = make_node(tmp_path, "b")
    (cfg_b.data_dir / "only-on-b.txt").write_bytes(b"bbbb")
    scan(cfg_b, index_b, metalog_b)

    app_a = create_app(cfg_a, index_a, metalog_a)
    peers_b = PeerStore(cfg_b.peers_path, static_peers=["http://node-a"])
    gossip_b = Gossip(cfg_b, index_b, metalog_b, peers_b,
                      transport=httpx.ASGITransport(app=app_a))

    asyncio.run(gossip_b.sync_peer("http://node-a"))

    # Pull: b learned a's file. Push: a learned b's file.
    assert index_b.get("/only-on-a.txt") is not None
    assert index_b.holders("/only-on-a.txt") == ["a"]
    assert index_a.get("/only-on-b.txt") is not None
    assert index_a.holders("/only-on-b.txt") == ["b"]
    # b learned a's node id from the pull, for holder -> URL resolution.
    assert peers_b.url_for_node("a") == "http://node-a"

    # A second round moves nothing new and does not grow the meta logs.
    lines_a = len(list(metalog_a.read_all()))
    lines_b = len(list(metalog_b.read_all()))
    asyncio.run(gossip_b.sync_peer("http://node-a"))
    assert len(list(metalog_a.read_all())) == lines_a
    assert len(list(metalog_b.read_all())) == lines_b


def test_fetch_then_open(tmp_path: Path):
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a")
    (cfg_a.data_dir / "models").mkdir()
    (cfg_a.data_dir / "models" / "m.gguf").write_bytes(b"weights")
    scan(cfg_a, index_a, metalog_a)
    app_a = create_app(cfg_a, index_a, metalog_a)

    cfg_b, index_b, metalog_b = make_node(tmp_path, "b")
    # b's index knows the record and holder (as if gossiped).
    delta = index_a.changes_since(0)
    merge_delta(cfg_b, index_b, metalog_b, [r.to_dict() for r in delta[0]], delta[1])

    peers_b = PeerStore(cfg_b.peers_path, static_peers=["http://node-a"])
    peers_b.note_node("http://node-a", "a")
    fetcher = Fetcher(cfg_b, index_b, peers_b,
                      client_factory=lambda url: TestClient(app_a))

    local = fetcher.open_path("/models/m.gguf")
    assert local == cfg_b.cache_dir / "models/m.gguf"
    assert local.read_bytes() == b"weights"
    # The cached copy counts toward redundancy: b is now a holder.
    assert sorted(index_b.holders("/models/m.gguf")) == ["a", "b"]

    # Second open serves straight from cache (no client factory needed).
    fetcher_no_net = Fetcher(cfg_b, index_b, peers_b,
                             client_factory=lambda url: (_ for _ in ()).throw(AssertionError))
    assert fetcher_no_net.open_path("/models/m.gguf") == local

    with pytest.raises(FileNotFoundError):
        fetcher.open_path("/nope")


def test_fetch_no_reachable_holder(tmp_path: Path):
    cfg_b, index_b, _ = make_node(tmp_path, "b")
    index_b.upsert(Record(path="/f", lamport=1, node="a", state="live", hash="blake3:x", size=1))
    index_b.set_holder("/f", "a")
    peers_b = PeerStore(cfg_b.peers_path)  # no URL known for node a
    fetcher = Fetcher(cfg_b, index_b, peers_b)
    with pytest.raises(IOError):
        fetcher.open_path("/f")


def test_blob_served_from_cache(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    rec = Record(path="/c.txt", lamport=1, node="b", state="live",
                 hash="blake3:whatever", size=6, mtime="t")
    index.upsert(rec)
    (cfg.cache_dir / "c.txt").write_bytes(b"cached")
    client = TestClient(create_app(cfg, index, metalog))
    resp = client.get("/v1/blob/blake3:whatever")
    assert resp.status_code == 200 and resp.content == b"cached"


def test_union_namespace(tmp_path: Path):
    index = Index(tmp_path / "idx.sqlite")
    index.upsert(Record(path="/models/m.gguf", lamport=1, node="a", state="live", size=7, mtime="t1"))
    index.upsert(Record(path="/photos/2026/trip.jpg", lamport=2, node="b", state="live", size=9, mtime="t2"))
    index.upsert(Record(path="/gone.txt", lamport=3, node="a", state="tombstone"))

    root = list_dir(index, "/")
    assert [(e["name"], e["type"]) for e in root] == [("models", "dir"), ("photos", "dir")]

    models = list_dir(index, "/models")
    assert models == [{"name": "m.gguf", "type": "file", "size": 7, "mtime": "t1"}]

    assert list_dir(index, "/photos") == [{"name": "2026", "type": "dir"}]
    assert list_dir(index, "/nope") is None

    assert stat_path(index, "/")["type"] == "dir"
    assert stat_path(index, "/photos/2026")["type"] == "dir"
    assert stat_path(index, "/models/m.gguf")["size"] == 7
    assert stat_path(index, "/gone.txt") is None
    assert stat_path(index, "/nope") is None


def test_ls_stat_and_file_endpoints(tmp_path: Path):
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a")
    (cfg_a.data_dir / "docs").mkdir()
    (cfg_a.data_dir / "docs" / "readme.md").write_bytes(b"# hi")
    scan(cfg_a, index_a, metalog_a)

    peers = PeerStore(cfg_a.peers_path)
    fetcher = Fetcher(cfg_a, index_a, peers)
    client = TestClient(create_app(cfg_a, index_a, metalog_a, fetcher=fetcher))

    ls = client.get("/v1/ls", params={"path": "/"}).json()
    assert ls["entries"] == [{"name": "docs", "type": "dir"}]
    assert client.get("/v1/ls", params={"path": "/nope"}).status_code == 404

    st = client.get("/v1/stat", params={"path": "/docs/readme.md"}).json()
    assert st["type"] == "file" and st["size"] == 4

    # Local file served through the fetch-then-open path.
    resp = client.get("/v1/file", params={"path": "/docs/readme.md"})
    assert resp.status_code == 200 and resp.content == b"# hi"
    assert client.get("/v1/file", params={"path": "/nope"}).status_code == 404


def test_file_endpoint_fetches_across_nodes(tmp_path: Path):
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a")
    (cfg_a.data_dir / "big.bin").write_bytes(b"x" * 4096)
    scan(cfg_a, index_a, metalog_a)
    app_a = create_app(cfg_a, index_a, metalog_a)

    cfg_b, index_b, metalog_b = make_node(tmp_path, "b")
    delta = index_a.changes_since(0)
    merge_delta(cfg_b, index_b, metalog_b, [r.to_dict() for r in delta[0]], delta[1])
    peers_b = PeerStore(cfg_b.peers_path)
    peers_b.note_node("http://node-a", "a")
    fetcher_b = Fetcher(cfg_b, index_b, peers_b, client_factory=lambda url: TestClient(app_a))

    client_b = TestClient(create_app(cfg_b, index_b, metalog_b, fetcher=fetcher_b))
    resp = client_b.get("/v1/file", params={"path": "/big.bin"})
    assert resp.status_code == 200 and resp.content == b"x" * 4096
    # The bytes landed in b's cache and b registered as a holder.
    assert (cfg_b.cache_dir / "big.bin").is_file()
    assert "b" in index_b.holders("/big.bin")


def test_peer_store_roundtrip(tmp_path: Path):
    store = PeerStore(tmp_path / "peers.json", static_peers=["http://one"])
    store.add("http://two")
    store.note_node("http://two", "node-two")
    assert store.urls() == ["http://one", "http://two"]
    assert store.url_for_node("node-two") == "http://two"

    # A fresh store re-reads the cached last-known-peers file.
    reloaded = PeerStore(tmp_path / "peers.json")
    assert reloaded.urls() == ["http://one", "http://two"]
    assert reloaded.url_for_node("node-two") == "http://two"
