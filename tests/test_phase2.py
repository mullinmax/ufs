"""Phase 2: write path, replica push, no-isolated-edits guard, reconciler."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dfs.api import create_app
from dfs.config import Config
from dfs.index import Index, Record
from dfs.metalog import MetaLog
from dfs.peers import PeerStore
from dfs.reconcile import Reconciler
from dfs.scanner import scan
from dfs.writer import (
    IsolatedWriteError,
    WriteThresholdError,
    Writer,
    decode_record_header,
    encode_record_header,
    receive_blob,
)


def make_node(tmp_path: Path, node_id: str, **cfg_overrides):
    cfg = Config()
    cfg.node_id = node_id
    cfg.data_dir = tmp_path / node_id / "data"
    cfg.control_dir = tmp_path / node_id / ".dfs"
    for key, value in cfg_overrides.items():
        setattr(cfg, key, value)
    cfg.ensure_dirs()
    return cfg, Index(cfg.index_path), MetaLog(cfg.records_log)


def make_pair(tmp_path: Path, **cfg_overrides):
    """Two nodes; a knows b as a peer, with apps wired via TestClient."""
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a", **cfg_overrides)
    cfg_b, index_b, metalog_b = make_node(tmp_path, "b", **cfg_overrides)
    app_b = create_app(cfg_b, index_b, metalog_b)
    peers_a = PeerStore(cfg_a.peers_path, static_peers=["http://node-b"])
    writer_a = Writer(cfg_a, index_a, metalog_a, peers_a,
                      client_factory=lambda url: TestClient(app_b))
    return (cfg_a, index_a, metalog_a, writer_a, peers_a), (cfg_b, index_b, metalog_b, app_b)


def test_record_header_roundtrip():
    rec = Record(path="/ünïcode/f.txt", lamport=4, node="a", state="live",
                 hash="blake3:x", size=9, mtime="t")
    assert decode_record_header(encode_record_header(rec)) == rec


def test_write_commits_and_replicates(tmp_path: Path):
    (a, b) = make_pair(tmp_path)
    cfg_a, index_a, metalog_a, writer_a, peers_a = a
    cfg_b, index_b, metalog_b, _ = b

    buffered = writer_a.buffer()
    buffered.write_bytes(b"hello world")
    record = writer_a.write("/docs/hi.txt", buffered)

    # Committed locally: bytes in /data, record in index and meta log.
    assert (cfg_a.data_dir / "docs/hi.txt").read_bytes() == b"hello world"
    assert index_a.get("/docs/hi.txt").hash == record.hash
    assert record in list(metalog_a.read_all())

    # Replicated synchronously to b (threshold 2): bytes, record, holder.
    assert (cfg_b.data_dir / "docs/hi.txt").read_bytes() == b"hello world"
    assert index_b.get("/docs/hi.txt") == record
    assert record in list(metalog_b.read_all())
    assert sorted(index_a.holders("/docs/hi.txt")) == ["a", "b"]
    assert index_b.holders("/docs/hi.txt") == ["b"]

    # a learned b's node id from the health check.
    assert peers_a.url_for_node("b") == "http://node-b"


def test_overwrite_bumps_version(tmp_path: Path):
    (a, b) = make_pair(tmp_path)
    cfg_a, index_a, metalog_a, writer_a, _ = a

    first = writer_a.buffer()
    first.write_bytes(b"v1")
    r1 = writer_a.write("/f.txt", first)
    second = writer_a.buffer()
    second.write_bytes(b"v2 longer")
    r2 = writer_a.write("/f.txt", second)

    assert r2.lamport > r1.lamport
    assert index_a.get("/f.txt").size == 9
    assert (cfg_a.data_dir / "f.txt").read_bytes() == b"v2 longer"
    assert b[1].get("/f.txt").hash == r2.hash


def test_isolated_write_refused(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    peers = PeerStore(cfg.peers_path)  # nobody to reach
    writer = Writer(cfg, index, metalog, peers)
    buffered = writer.buffer()
    buffered.write_bytes(b"data")
    with pytest.raises(IsolatedWriteError):
        writer.write("/f.txt", buffered)
    # Nothing committed: no bytes, no record, buffer cleaned up.
    assert not (cfg.data_dir / "f.txt").exists()
    assert index.get("/f.txt") is None
    assert not buffered.exists()


def test_threshold_one_allows_single_node(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", write_threshold=1)
    writer = Writer(cfg, index, metalog, PeerStore(cfg.peers_path))
    buffered = writer.buffer()
    buffered.write_bytes(b"solo")
    record = writer.write("/f.txt", buffered)
    assert record.state == "live"
    assert index.holders("/f.txt") == ["a"]


def test_unreachable_peer_after_commit_raises_threshold_error(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    peers = PeerStore(cfg.peers_path, static_peers=["http://node-b"])

    calls = {"n": 0}

    class FlakyClient:
        """Health answers (so the guard passes), but the push then fails."""

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def get(self, path, **kw):
            import httpx
            return httpx.Response(200, json={"node": "b", "free_bytes": 1},
                                  request=httpx.Request("GET", "http://node-b" + path))

        def post(self, path, **kw):
            import httpx
            raise httpx.ConnectError("gone")

    writer = Writer(cfg, index, metalog, peers, client_factory=lambda url: FlakyClient())
    buffered = writer.buffer()
    buffered.write_bytes(b"data")
    with pytest.raises(WriteThresholdError):
        writer.write("/f.txt", buffered)
    # The local commit stays (it will gossip and reconcile later).
    assert (cfg.data_dir / "f.txt").read_bytes() == b"data"
    assert index.holders("/f.txt") == ["a"]


def test_receive_blob_rejects_hash_mismatch(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    bad = cfg.tmp_dir / "recv"
    bad.write_bytes(b"tampered")
    rec = Record(path="/f", lamport=1, node="b", state="live", hash="blake3:nope", size=8)
    with pytest.raises(ValueError):
        receive_blob(cfg, index, metalog, rec, bad)
    assert index.get("/f") is None and not bad.exists()


def test_put_file_endpoint(tmp_path: Path):
    (a, b) = make_pair(tmp_path)
    cfg_a, index_a, metalog_a, writer_a, _ = a
    client_a = TestClient(create_app(cfg_a, index_a, metalog_a, writer=writer_a))

    resp = client_a.put("/v1/file", params={"path": "/photos/x.jpg"}, content=b"jpegbytes")
    assert resp.status_code == 200
    body = resp.json()
    assert body["record"]["state"] == "live" and body["record"]["size"] == 9
    assert sorted(body["holders"]) == ["a", "b"]
    assert (b[0].data_dir / "photos/x.jpg").read_bytes() == b"jpegbytes"

    assert client_a.put("/v1/file", params={"path": "../evil"}, content=b"x").status_code == 400
    assert client_a.put("/v1/file", params={"path": "/a/../evil"}, content=b"x").status_code == 400


def test_put_file_isolated_returns_503(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    writer = Writer(cfg, index, metalog, PeerStore(cfg.peers_path))
    client = TestClient(create_app(cfg, index, metalog, writer=writer))
    resp = client.put("/v1/file", params={"path": "/f.txt"}, content=b"x")
    assert resp.status_code == 503


def test_post_blob_endpoint(tmp_path: Path):
    from dfs.hashing import hash_file

    cfg, index, metalog = make_node(tmp_path, "b")
    client = TestClient(create_app(cfg, index, metalog))

    src = tmp_path / "src.bin"
    src.write_bytes(b"payload")
    rec = Record(path="/p.bin", lamport=1, node="a", state="live",
                 hash=hash_file(src), size=7, mtime="t")
    resp = client.post("/v1/blob", content=b"payload",
                       headers={"x-dfs-record": encode_record_header(rec)})
    assert resp.status_code == 200 and resp.json() == {"stored": True, "node": "b"}
    assert (cfg.data_dir / "p.bin").read_bytes() == b"payload"
    assert index.get("/p.bin") == rec
    assert index.holders("/p.bin") == ["b"]

    assert client.post("/v1/blob", content=b"x").status_code == 400
    resp = client.post("/v1/blob", content=b"wrong",
                       headers={"x-dfs-record": encode_record_header(rec)})
    assert resp.status_code == 400


def test_reconciler_tops_up_to_n(tmp_path: Path):
    # a holds a file alone; N=2; the reconciler pushes a copy to b.
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a", n_copies=2)
    (cfg_a.data_dir / "lonely.txt").write_bytes(b"only one copy")
    scan(cfg_a, index_a, metalog_a)

    cfg_b, index_b, metalog_b = make_node(tmp_path, "b", n_copies=2)
    app_b = create_app(cfg_b, index_b, metalog_b)
    peers_a = PeerStore(cfg_a.peers_path, static_peers=["http://node-b"])
    factory = lambda url: TestClient(app_b)
    writer_a = Writer(cfg_a, index_a, metalog_a, peers_a, client_factory=factory)
    reconciler = Reconciler(cfg_a, index_a, writer_a, peers_a, client_factory=factory)

    assert reconciler.round() == 1
    assert (cfg_b.data_dir / "lonely.txt").read_bytes() == b"only one copy"
    assert sorted(index_a.holders("/lonely.txt")) == ["a", "b"]
    assert index_b.holders("/lonely.txt") == ["b"]

    # Already at N: the next round is a no-op.
    assert reconciler.round() == 0


def test_reconciler_skips_paths_it_does_not_hold(tmp_path: Path):
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a", n_copies=3)
    # a knows about a record held elsewhere but has no local bytes.
    index_a.upsert(Record(path="/far.bin", lamport=1, node="c", state="live",
                          hash="blake3:x", size=1))
    index_a.set_holder("/far.bin", "c")

    cfg_b, index_b, metalog_b = make_node(tmp_path, "b")
    app_b = create_app(cfg_b, index_b, metalog_b)
    peers_a = PeerStore(cfg_a.peers_path, static_peers=["http://node-b"])
    factory = lambda url: TestClient(app_b)
    writer_a = Writer(cfg_a, index_a, metalog_a, peers_a, client_factory=factory)
    reconciler = Reconciler(cfg_a, index_a, writer_a, peers_a, client_factory=factory)
    assert reconciler.round() == 0
    assert index_b.get("/far.bin") is None


def test_write_invalidates_stale_cache(tmp_path: Path):
    (a, b) = make_pair(tmp_path)
    cfg_a, index_a, metalog_a, writer_a, _ = a
    stale = cfg_a.cache_dir / "f.txt"
    stale.write_bytes(b"old cached bytes")
    buffered = writer_a.buffer()
    buffered.write_bytes(b"new")
    writer_a.write("/f.txt", buffered)
    assert not stale.exists()
