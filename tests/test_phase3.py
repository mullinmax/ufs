"""Phase 3: tombstones, propagation with byte purging, delete-then-re-add,
straggler reconciliation on rejoin."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dfs.api import create_app
from dfs.config import Config
from dfs.delete import Deleter, apply_remote_record
from dfs.gossip import merge_delta
from dfs.index import Index, Record
from dfs.metalog import MetaLog
from dfs.peers import PeerStore
from dfs.reconcile import Reconciler
from dfs.scanner import scan
from dfs.writer import IsolatedWriteError, Writer


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
    """Two nodes; a knows b as a peer, with writer and deleter wired to b's app."""
    cfg_a, index_a, metalog_a = make_node(tmp_path, "a", **cfg_overrides)
    cfg_b, index_b, metalog_b = make_node(tmp_path, "b", **cfg_overrides)
    app_b = create_app(cfg_b, index_b, metalog_b)
    peers_a = PeerStore(cfg_a.peers_path, static_peers=["http://node-b"])
    writer_a = Writer(cfg_a, index_a, metalog_a, peers_a,
                      client_factory=lambda url: TestClient(app_b))
    deleter_a = Deleter(cfg_a, index_a, metalog_a, writer_a)
    return (cfg_a, index_a, metalog_a, writer_a, deleter_a), (cfg_b, index_b, metalog_b)


def write(writer: Writer, path: str, data: bytes) -> Record:
    buffered = writer.buffer()
    buffered.write_bytes(data)
    return writer.write(path, buffered)


def test_delete_tombstones_purges_and_propagates(tmp_path: Path):
    (a, b) = make_pair(tmp_path)
    cfg_a, index_a, metalog_a, writer_a, deleter_a = a
    cfg_b, index_b, metalog_b = b
    record = write(writer_a, "/docs/x.txt", b"bytes")
    assert (cfg_b.data_dir / "docs/x.txt").is_file()

    tombstone = deleter_a.delete("/docs/x.txt")

    # A tombstone at a higher version, durable in the meta log.
    assert tombstone.state == "tombstone"
    assert tombstone.lamport > record.lamport
    assert tombstone.hash == record.hash
    assert tombstone in list(metalog_a.read_all())
    assert index_a.get("/docs/x.txt").state == "tombstone"

    # Local bytes and holders are gone on a.
    assert not (cfg_a.data_dir / "docs/x.txt").exists()
    assert index_a.holders("/docs/x.txt") == []

    # The eager tombstone push made b purge its copy too.
    assert not (cfg_b.data_dir / "docs/x.txt").exists()
    assert index_b.get("/docs/x.txt") == tombstone
    assert index_b.holders("/docs/x.txt") == []
    assert tombstone in list(metalog_b.read_all())


def test_delete_missing_or_tombstoned_raises(tmp_path: Path):
    (a, _) = make_pair(tmp_path)
    _, _, _, writer_a, deleter_a = a
    with pytest.raises(FileNotFoundError):
        deleter_a.delete("/never-existed")
    write(writer_a, "/f.txt", b"x")
    deleter_a.delete("/f.txt")
    with pytest.raises(FileNotFoundError):
        deleter_a.delete("/f.txt")  # already a tombstone


def test_isolated_delete_refused(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    (cfg.data_dir / "f.txt").write_bytes(b"data")
    scan(cfg, index, metalog)
    writer = Writer(cfg, index, metalog, PeerStore(cfg.peers_path))  # nobody to reach
    deleter = Deleter(cfg, index, metalog, writer)
    with pytest.raises(IsolatedWriteError):
        deleter.delete("/f.txt")
    # Nothing changed: bytes intact, record still live.
    assert (cfg.data_dir / "f.txt").read_bytes() == b"data"
    assert index.get("/f.txt").state == "live"


def test_threshold_one_allows_single_node_delete(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", write_threshold=1)
    (cfg.data_dir / "f.txt").write_bytes(b"data")
    scan(cfg, index, metalog)
    writer = Writer(cfg, index, metalog, PeerStore(cfg.peers_path))
    deleter = Deleter(cfg, index, metalog, writer)
    tombstone = deleter.delete("/f.txt")
    assert tombstone.state == "tombstone"
    assert not (cfg.data_dir / "f.txt").exists()


def test_delete_then_identical_re_add(tmp_path: Path):
    (a, b) = make_pair(tmp_path)
    cfg_a, index_a, _, writer_a, deleter_a = a

    r1 = write(writer_a, "/f.txt", b"same content")
    tombstone = deleter_a.delete("/f.txt")
    r2 = write(writer_a, "/f.txt", b"same content")

    # Byte-identical re-add is live at a version above the tombstone;
    # the version decides, content is irrelevant (design doc §10).
    assert r2.state == "live" and r2.hash == r1.hash
    assert r2.lamport > tombstone.lamport
    assert index_a.get("/f.txt") == r2
    assert (cfg_a.data_dir / "f.txt").read_bytes() == b"same content"
    # The re-add gossips normally: a stale tombstone can never suppress it.
    cfg_b, index_b, metalog_b = b
    assert index_b.get("/f.txt") == r2
    assert (cfg_b.data_dir / "f.txt").read_bytes() == b"same content"


def test_tombstone_merge_purges_straggler(tmp_path: Path):
    # Straggler c held a copy while offline; on rejoin the gossiped
    # tombstone (higher version) purges its bytes and holder set.
    cfg_c, index_c, metalog_c = make_node(tmp_path, "c")
    (cfg_c.data_dir / "stale.txt").write_bytes(b"old copy")
    (cfg_c.cache_dir / "stale.txt").write_bytes(b"old copy")
    scan(cfg_c, index_c, metalog_c)
    held = index_c.get("/stale.txt")

    tombstone = Record(path="/stale.txt", lamport=held.lamport + 1, node="a",
                       state="tombstone", hash=held.hash, mtime="t")
    accepted = merge_delta(cfg_c, index_c, metalog_c,
                           [tombstone.to_dict()], [{"path": "/stale.txt", "node": "a"}])
    assert accepted == 1
    assert not (cfg_c.data_dir / "stale.txt").exists()
    assert not (cfg_c.cache_dir / "stale.txt").exists()
    assert index_c.get("/stale.txt") == tombstone
    # The holder entry gossiped alongside is stale and was skipped.
    assert index_c.holders("/stale.txt") == []

    # An older tombstone never purges a newer live record.
    cfg_d, index_d, metalog_d = make_node(tmp_path, "d")
    (cfg_d.data_dir / "kept.txt").write_bytes(b"new")
    scan(cfg_d, index_d, metalog_d)
    live = index_d.get("/kept.txt")
    old_tomb = Record(path="/kept.txt", lamport=live.lamport - 1, node="a",
                      state="tombstone", mtime="t")
    assert merge_delta(cfg_d, index_d, metalog_d, [old_tomb.to_dict()], []) == 0
    assert (cfg_d.data_dir / "kept.txt").read_bytes() == b"new"
    assert index_d.get("/kept.txt") == live


def test_newer_live_record_drops_stale_copy(tmp_path: Path):
    # Straggler reconciliation, live case: a higher-versioned live record
    # means the local copy is stale — drop it and stop claiming holdership.
    cfg, index, metalog = make_node(tmp_path, "c")
    (cfg.data_dir / "f.txt").write_bytes(b"old bytes")
    scan(cfg, index, metalog)
    stale = index.get("/f.txt")

    newer = Record(path="/f.txt", lamport=stale.lamport + 1, node="a",
                   state="live", hash="blake3:different", size=3, mtime="t")
    merge_delta(cfg, index, metalog, [newer.to_dict()], [{"path": "/f.txt", "node": "a"}])
    assert not (cfg.data_dir / "f.txt").exists()
    assert index.get("/f.txt") == newer
    assert index.holders("/f.txt") == ["a"]


def test_matching_bytes_survive_newer_record(tmp_path: Path):
    # If the local bytes already match the new version's hash, keep them.
    from dfs.hashing import hash_file
    cfg, index, metalog = make_node(tmp_path, "c")
    (cfg.data_dir / "f.txt").write_bytes(b"same bytes")
    scan(cfg, index, metalog)
    held = index.get("/f.txt")

    newer = Record(path="/f.txt", lamport=held.lamport + 1, node="a",
                   state="live", hash=held.hash, size=held.size, mtime="t")
    merge_delta(cfg, index, metalog, [newer.to_dict()], [])
    assert (cfg.data_dir / "f.txt").read_bytes() == b"same bytes"
    assert "c" in index.holders("/f.txt")


def test_reconciler_ignores_tombstones(tmp_path: Path):
    (a, b) = make_pair(tmp_path, n_copies=2)
    cfg_a, index_a, metalog_a, writer_a, deleter_a = a
    write(writer_a, "/f.txt", b"x")
    deleter_a.delete("/f.txt")
    peers_a = writer_a.peers
    reconciler = Reconciler(cfg_a, index_a, writer_a, peers_a)
    assert reconciler.round() == 0


def test_delete_file_endpoint(tmp_path: Path):
    (a, b) = make_pair(tmp_path)
    cfg_a, index_a, metalog_a, writer_a, deleter_a = a
    client_a = TestClient(create_app(cfg_a, index_a, metalog_a,
                                     writer=writer_a, deleter=deleter_a))

    client_a.put("/v1/file", params={"path": "/x.txt"}, content=b"bytes")
    resp = client_a.delete("/v1/file", params={"path": "/x.txt"})
    assert resp.status_code == 200
    assert resp.json()["record"]["state"] == "tombstone"
    # Deleted paths disappear from the namespace and locate.
    assert client_a.get("/v1/stat", params={"path": "/x.txt"}).status_code == 404
    assert client_a.get("/v1/locate", params={"path": "/x.txt"}).status_code == 404
    assert client_a.delete("/v1/file", params={"path": "/x.txt"}).status_code == 404
    assert client_a.delete("/v1/file", params={"path": "/nope"}).status_code == 404


def test_delete_file_isolated_returns_503(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    (cfg.data_dir / "f.txt").write_bytes(b"x")
    scan(cfg, index, metalog)
    writer = Writer(cfg, index, metalog, PeerStore(cfg.peers_path))
    deleter = Deleter(cfg, index, metalog, writer)
    client = TestClient(create_app(cfg, index, metalog, deleter=deleter))
    assert client.delete("/v1/file", params={"path": "/f.txt"}).status_code == 503


def test_rejoin_scan_then_gossip_settles(tmp_path: Path):
    # A straggler restarts: the scan re-registers its stale file (same
    # version, from its own meta log), then the gossiped tombstone lands
    # and purges it — rejoin converges to the deleted state.
    cfg, index, metalog = make_node(tmp_path, "c")
    (cfg.data_dir / "f.txt").write_bytes(b"held while offline")
    scan(cfg, index, metalog)
    held = index.get("/f.txt")

    # Simulated restart: fresh index rebuilt from meta log + disk.
    index.close()
    cfg.index_path.unlink()
    index = Index(cfg.index_path)
    scan(cfg, index, metalog)
    assert index.get("/f.txt") == held  # no version bump on rescan

    tombstone = Record(path="/f.txt", lamport=held.lamport + 1, node="a",
                       state="tombstone", hash=held.hash, mtime="t")
    merge_delta(cfg, index, metalog, [tombstone.to_dict()], [])
    assert not (cfg.data_dir / "f.txt").exists()
    assert index.get("/f.txt") == tombstone
