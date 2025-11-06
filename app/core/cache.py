"""Redis cache helpers for memoizing agent results."""

from __future__ import annotations

import json
from typing import Any

import redis

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

_redis_client: redis.Redis | None = None

if settings.cache and settings.cache.redis_url:
    try:
        _redis_client = redis.Redis.from_url(
            settings.cache.redis_url,
            decode_responses=True,
        )
        # Smoke test connection (non fatal)
        _redis_client.ping()
    except Exception as exc:  # pragma: no cover - network issues handled at runtime
        logger.warning("Redis disabled due to connection failure: %s", exc)
        _redis_client = None
else:
    logger.info("Redis cache not configured.")


def get_client() -> redis.Redis | None:
    return _redis_client


def get_json(key: str) -> Any | None:
    """Retrieve JSON payload from cache."""
    if not _redis_client:
        return None
    try:
        value = _redis_client.get(key)
    except Exception as exc:  # pragma: no cover
        logger.warning("Redis get failed for key %s: %s", key, exc)
        return None
    return json.loads(value) if value else None


def set_json(key: str, payload: Any, ttl: int | None = None) -> None:
    """Store JSON payload in cache with optional TTL."""
    if not _redis_client:
        return
    try:
        ttl_seconds = ttl or (settings.cache.ttl_seconds if settings.cache else None)
        data = json.dumps(payload)
        if ttl_seconds:
            _redis_client.setex(key, ttl_seconds, data)
        else:
            _redis_client.set(key, data)
    except Exception as exc:  # pragma: no cover
        logger.warning("Redis set failed for key %s: %s", key, exc)


def delete(key: str) -> None:
    if not _redis_client:
        return
    try:
        _redis_client.delete(key)
    except Exception as exc:  # pragma: no cover
        logger.warning("Redis delete failed for key %s: %s", key, exc)

