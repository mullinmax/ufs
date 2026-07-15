import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dfs.api import create_app
from dfs.auth import sign, verify
from dfs.config import Config
from dfs.hashing import hash_file
from dfs.index import Index, Record
from dfs.metalog import MetaLog
from dfs.scanner import scan


@pytest.fixture
def config(tmp_path: Path) -> Config:
    cfg = Config()
    cfg.node_id = "test-node"
    cfg.data_dir = tmp_path / "data"
    cfg.control_dir = tmp_path / ".dfs"
    cfg.ensure_dirs()
    return cfg


def make_env(config: Config):
    index = Index(config.index_path)
    metalog = MetaLog(config.records_log)
    return index, metalog


def test_hash_file(tmp_path: Path):
    f = tmp_path / "a.txt"
    f.write_bytes(b"hello world")
    h = hash_file(f)
    assert h.startswith("blake3:")
    assert h == hash_file(f)


def test_scan_and_rescan(config: Config):
    (config.data_dir / "models").mkdir()
    (config.data_dir / "models" / "m.gguf").write_bytes(b"weights")
    index, metalog = make_env(config)
    assert scan(config, index, metalog) == 1
    rec = index.get("/models/m.gguf")
    assert rec is not None and rec.state == "live" and rec.size == 7
    assert index.holders("/models/m.gguf") == ["test-node"]
    # Rescan is idempotent: no new versions for unchanged files.
    lamport_before = rec.lamport
    scan(config, index, metalog)
    assert index.get("/models/m.gguf").lamport == lamport_before


def test_index_lww_merge(tmp_path: Path):
    index = Index(tmp_path / "idx.sqlite")
    old = Record(path="/f", lamport=1, node="a", state="live", hash="blake3:x", size=1)
    new = Record(path="/f", lamport=2, node="b", state="tombstone")
    assert index.upsert(old)
    assert index.upsert(new)
    assert not index.upsert(old)  # stale version loses
    assert index.get("/f").state == "tombstone"


def test_metalog_roundtrip(config: Config):
    _, metalog = make_env(config)
    rec = Record(path="/x", lamport=5, node="n", state="live", hash="blake3:y", size=3, mtime="t")
    metalog.append(rec)
    assert list(metalog.read_all()) == [rec]


def test_auth():
    token = sign("secret", "GET", "/v1/health")
    assert verify("secret", "GET", "/v1/health", token)
    assert not verify("secret", "GET", "/v1/health", "bogus")
    assert verify("", "GET", "/v1/health", "")  # dev mode: no secret configured


def test_api_endpoints(config: Config):
    (config.data_dir / "f.txt").write_bytes(b"data123")
    index, metalog = make_env(config)
    scan(config, index, metalog)
    client = TestClient(create_app(config, index, metalog))

    health = client.get("/v1/health").json()
    assert health["node"] == "test-node"

    records = client.get("/v1/index").json()["records"]
    assert records[0]["path"] == "/f.txt"

    locate = client.get("/v1/locate", params={"path": "/f.txt"}).json()
    assert locate["holders"] == ["test-node"]
    assert client.get("/v1/locate", params={"path": "/nope"}).status_code == 404

    blob = client.get(f"/v1/blob/{records[0]['hash']}")
    assert blob.content == b"data123"

    ranged = client.get(f"/v1/blob/{records[0]['hash']}", headers={"Range": "bytes=0-3"})
    assert ranged.status_code == 206
    assert ranged.content == b"data"


def test_api_rejects_bad_token(config: Config):
    config.cluster_secret = "s3cret"
    index, metalog = make_env(config)
    client = TestClient(create_app(config, index, metalog))
    assert client.get("/v1/health").status_code == 403
    token = sign("s3cret", "GET", "/v1/health")
    assert client.get("/v1/health", headers={"x-dfs-token": token}).status_code == 200
