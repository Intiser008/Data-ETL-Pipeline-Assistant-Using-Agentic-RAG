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


def get_json_list(key: str) -> list[Any]:
    """Retrieve a list stored as JSON; return empty list when missing."""
    payload = get_json(key)
    if isinstance(payload, list):
        return payload
    if payload is None:
        return []
    return [payload]


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


def append_json_list(key: str, item: Any, *, ttl: int | None = None, max_items: int | None = None) -> None:
    """Append an item to a JSON list stored at key."""
    current = get_json_list(key)
    current.append(item)
    if max_items and len(current) > max_items:
        current = current[-max_items:]
    set_json(key, current, ttl=ttl)


def delete(key: str) -> None:
    if not _redis_client:
        return
    try:
        _redis_client.delete(key)
    except Exception as exc:  # pragma: no cover
        logger.warning("Redis delete failed for key %s: %s", key, exc)

