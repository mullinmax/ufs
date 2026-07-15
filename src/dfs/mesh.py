"""Headscale/WireGuard mesh join and tailnet peer discovery (design doc §5).

The mesh itself is provided by the local tailscale client pointed at a
self-hosted Headscale server. The agent's job is only to (a) join the tailnet
on startup if it isn't already joined, and (b) read the tailnet peer list so
gossip can reach every member. Both are best-effort: if the tailscale binary
is absent or Headscale is down, the agent keeps running on whatever peers it
already knows (existing links keep working; only new joins are blocked).
"""

import json
import logging
import shutil
import subprocess

from .config import Config
from .peers import PeerStore

log = logging.getLogger("dfs.mesh")


def _tailscale_status() -> dict | None:
    if shutil.which("tailscale") is None:
        return None
    try:
        out = subprocess.run(
            ["tailscale", "status", "--json"],
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout
        return json.loads(out)
    except (subprocess.SubprocessError, json.JSONDecodeError) as exc:
        log.warning("tailscale status failed: %s", exc)
        return None


def join_mesh(config: Config) -> None:
    """Join the Headscale tailnet if configured and not already joined."""
    if not config.headscale_url or not config.headscale_authkey:
        return
    if shutil.which("tailscale") is None:
        log.warning("DFS_HEADSCALE_URL set but no tailscale binary found; skipping mesh join")
        return
    status = _tailscale_status()
    if status and status.get("BackendState") == "Running":
        log.info("already joined mesh (tailscale running)")
        return
    log.info("joining mesh via %s", config.headscale_url)
    try:
        subprocess.run(
            [
                "tailscale", "up",
                "--login-server", config.headscale_url,
                "--authkey", config.headscale_authkey,
                "--hostname", config.node_id,
            ],
            capture_output=True, text=True, timeout=60, check=True,
        )
    except subprocess.SubprocessError as exc:
        log.warning("mesh join failed (pool keeps running on known peers): %s", exc)


def discover_peers(config: Config, peers: PeerStore) -> None:
    """Add tailnet peers (by mesh IP, on our agent port) to the peer store."""
    status = _tailscale_status()
    if not status:
        return
    for peer in (status.get("Peer") or {}).values():
        for ip in peer.get("TailscaleIPs") or []:
            if ":" in ip:  # skip IPv6 for URL simplicity
                continue
            peers.add(f"http://{ip}:{config.listen_port}")
