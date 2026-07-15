"""Cluster-secret HMAC auth for agent-to-agent requests (design doc §5)."""

import hashlib
import hmac


def sign(cluster_secret: str, method: str, path: str) -> str:
    msg = f"{method.upper()} {path}".encode()
    return hmac.new(cluster_secret.encode(), msg, hashlib.sha256).hexdigest()


def verify(cluster_secret: str, method: str, path: str, token: str) -> bool:
    if not cluster_secret:
        # No secret configured: single-node / dev mode, allow everything.
        return True
    return hmac.compare_digest(sign(cluster_secret, method, path), token)
