"""Phase 4: redundancy-aware LRU eviction, pinning (node.toml + API),
proactive pin warmup, DFS_CACHE_SIZE enforcement."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from dfs.api import create_app
from dfs.cache import CacheManager
from dfs.config import Config, parse_size
from dfs.fetch import Fetcher
from dfs.index import Index, Record
from dfs.metalog import MetaLog
from dfs.peers import PeerStore
from dfs.pins import PinConflictError, PinStore
from dfs.scanner import scan
from dfs.writer import Writer


def make_node(tmp_path: Path, node_id: str, **cfg_overrides):
    cfg = Config()
    cfg.node_id = node_id
    cfg.data_dir = tmp_path / node_id / "data"
    cfg.control_dir = tmp_path / node_id / ".dfs"
    for key, value in cfg_overrides.items():
        setattr(cfg, key, value)
    cfg.ensure_dirs()
    return cfg, Index(cfg.index_path), MetaLog(cfg.records_log)


def seed_cache(cfg, index, path: str, data: bytes, lamport: int = 1,
               holders: list[str] | None = None) -> Record:
    """A cached copy of `path` on this node, indexed with the given holders."""
    record = Record(path=path, lamport=lamport, node="origin", state="live",
                    hash=f"blake3:{path}", size=len(data), mtime="t")
    index.upsert(record)
    for holder in holders or [cfg.node_id]:
        index.set_holder(path, holder)
    cached = cfg.cache_dir / path.lstrip("/")
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(data)
    return record


def test_parse_size():
    assert parse_size("") == 0
    assert parse_size("0") == 0
    assert parse_size("1048576") == 1048576
    assert parse_size("4TB") == 4 * 10**12
    assert parse_size("500 GB") == 500 * 10**9
    assert parse_size("1.5kb") == 1500
    assert parse_size("2GiB") == 2 * 2**30
    with pytest.raises(ValueError):
        parse_size("lots")


def test_pins_from_node_toml_and_api(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    cfg.node_toml_path.write_text('[[pin]]\nprefix = "/models/"\n')
    pins = PinStore(cfg)

    assert pins.is_pinned("/models/foo.gguf")
    assert pins.is_pinned("/models")  # the prefix itself
    assert not pins.is_pinned("/models2/foo")  # literal sibling, not under the pin
    assert not pins.is_pinned("/other/foo")

    assert pins.add("/photos") is True
    assert pins.add("/photos") is False  # idempotent
    assert pins.is_pinned("/photos/2024/x.jpg")
    # Dynamic pins survive a restart via pins.json.
    assert PinStore(cfg).is_pinned("/photos/2024/x.jpg")

    pins.remove("/photos")
    assert not pins.is_pinned("/photos/x")
    with pytest.raises(KeyError):
        pins.remove("/photos")
    with pytest.raises(PinConflictError):
        pins.remove("/models/")  # operator-managed
    with pytest.raises(ValueError):
        pins.add("relative/path")


def test_pin_endpoints(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a")
    cfg.node_toml_path.write_text('[[pin]]\nprefix = "/models/"\n')
    client = TestClient(create_app(cfg, index, metalog, pins=PinStore(cfg)))

    resp = client.get("/v1/pin")
    assert resp.json()["pins"] == [{"prefix": "/models/", "source": "node.toml"}]

    resp = client.post("/v1/pin", params={"prefix": "/photos/"})
    assert resp.status_code == 200 and resp.json()["added"] is True
    assert {"prefix": "/photos/", "source": "api"} in resp.json()["pins"]

    assert client.delete("/v1/pin", params={"prefix": "/photos/"}).status_code == 200
    assert client.delete("/v1/pin", params={"prefix": "/photos/"}).status_code == 404
    assert client.delete("/v1/pin", params={"prefix": "/models/"}).status_code == 409
    assert client.post("/v1/pin", params={"prefix": "../etc"}).status_code == 400


def test_lru_eviction_respects_order_and_budget(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", n_copies=1, cache_size=100)
    pins = PinStore(cfg)
    cache = CacheManager(cfg, index, pins)
    # Three 60-byte entries, all safely replicated elsewhere (holders > N).
    for name in ("old", "mid", "new"):
        seed_cache(cfg, index, f"/{name}", b"x" * 60, holders=["a", "b"])
        cache.record_access(f"/{name}")

    evicted = cache.evict_if_needed()

    # 180 bytes over a 100-byte budget: the two least-recent go, oldest first.
    assert evicted == ["/old", "/mid"]
    assert not (cfg.cache_dir / "old").exists()
    assert (cfg.cache_dir / "new").is_file()
    # Holder claims for evicted copies were released.
    assert index.holders("/old") == ["b"]
    assert "a" in index.holders("/new")
    # Under budget now: another pass is a no-op.
    assert cache.evict_if_needed() == []


def test_eviction_never_drops_below_n(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", n_copies=2, cache_size=10)
    cache = CacheManager(cfg, index, PinStore(cfg))
    # Two holders and N=2: this cached copy is load-bearing, never evicted.
    seed_cache(cfg, index, "/scarce", b"x" * 50, holders=["a", "b"])
    # Three holders: evicting still leaves N.
    seed_cache(cfg, index, "/plentiful", b"y" * 50, holders=["a", "b", "c"])
    cache.record_access("/plentiful")  # scarce is older, would go first if allowed

    evicted = cache.evict_if_needed()

    assert evicted == ["/plentiful"]
    assert (cfg.cache_dir / "scarce").is_file()
    assert index.holders("/scarce") == ["a", "b"]


def test_eviction_skips_pinned(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", n_copies=1, cache_size=10)
    pins = PinStore(cfg)
    pins.add("/models")
    cache = CacheManager(cfg, index, pins)
    seed_cache(cfg, index, "/models/big.gguf", b"x" * 50, holders=["a", "b"])
    seed_cache(cfg, index, "/junk", b"y" * 50, holders=["a", "b"])

    evicted = cache.evict_if_needed()

    assert evicted == ["/junk"]
    assert (cfg.cache_dir / "models/big.gguf").is_file()


def test_eviction_of_stale_and_data_backed_entries(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", n_copies=3, cache_size=1)
    cache = CacheManager(cfg, index, PinStore(cfg))
    # Tombstoned leftovers and index-unknown files are always evictable.
    stale = seed_cache(cfg, index, "/gone", b"x" * 40)
    index.upsert(Record(path="/gone", lamport=stale.lamport + 1, node="b",
                        state="tombstone", mtime="t"))
    (cfg.cache_dir / "unknown").write_bytes(b"y" * 40)
    # A cache copy shadowed by a /data copy is redundant even when scarce:
    # eviction is allowed and keeps the holder claim (the /data copy stays).
    seed_cache(cfg, index, "/shadowed", b"z" * 40, holders=["a"])
    (cfg.data_dir / "shadowed").write_bytes(b"z" * 40)

    evicted = cache.evict_if_needed()

    assert sorted(evicted) == ["/gone", "/shadowed", "/unknown"]
    assert (cfg.data_dir / "shadowed").is_file()
    assert index.holders("/shadowed") == ["a"]


def test_unbounded_cache_never_evicts(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", n_copies=1, cache_size=0)
    cache = CacheManager(cfg, index, PinStore(cfg))
    seed_cache(cfg, index, "/f", b"x" * 10_000, holders=["a", "b"])
    assert cache.evict_if_needed() == []
    assert (cfg.cache_dir / "f").is_file()


def test_pin_warmup_fetches_from_peer(tmp_path: Path):
    # b holds the file; a pins the prefix and warms it into its cache.
    cfg_b, index_b, metalog_b = make_node(tmp_path, "b")
    (cfg_b.data_dir / "models/llm.gguf").parent.mkdir(parents=True)
    (cfg_b.data_dir / "models/llm.gguf").write_bytes(b"weights")
    scan(cfg_b, index_b, metalog_b)
    app_b = create_app(cfg_b, index_b, metalog_b)

    cfg_a, index_a, metalog_a = make_node(tmp_path, "a")
    record = index_b.get("/models/llm.gguf")
    index_a.upsert(record)
    index_a.set_holder("/models/llm.gguf", "b")
    peers_a = PeerStore(cfg_a.peers_path, static_peers=["http://node-b"])
    peers_a.note_node("http://node-b", "b")
    fetcher_a = Fetcher(cfg_a, index_a, peers_a,
                        client_factory=lambda url: TestClient(app_b))
    pins_a = PinStore(cfg_a)
    pins_a.add("/models/")
    cache_a = CacheManager(cfg_a, index_a, pins_a)
    fetcher_a.on_cache_access = cache_a.record_access

    assert cache_a.warm_pins(fetcher_a) == 1
    assert (cfg_a.cache_dir / "models/llm.gguf").read_bytes() == b"weights"
    assert "a" in index_a.holders("/models/llm.gguf")
    assert cache_a.warm_pins(fetcher_a) == 0  # already local: idempotent


def test_fetch_records_cache_access_and_lru_persists(tmp_path: Path):
    cfg, index, metalog = make_node(tmp_path, "a", n_copies=1, cache_size=100)
    cache = CacheManager(cfg, index, PinStore(cfg))
    fetcher = Fetcher(cfg, index, PeerStore(cfg.peers_path))
    fetcher.on_cache_access = cache.record_access
    seed_cache(cfg, index, "/first", b"x" * 60, holders=["a", "b"])
    cache.record_access("/first")
    seed_cache(cfg, index, "/second", b"y" * 60, holders=["a", "b"])
    cache.record_access("/second")

    # Serving /first from cache refreshes its LRU slot...
    assert fetcher.open_path("/first") == cfg.cache_dir / "first"
    # ...even across a restart (cache_lru.json), so /second evicts first.
    fresh = CacheManager(cfg, index, PinStore(cfg))
    assert fresh.evict_if_needed() == ["/second"]
    assert (cfg.cache_dir / "first").is_file()
