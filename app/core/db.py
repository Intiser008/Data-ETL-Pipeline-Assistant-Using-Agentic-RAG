"""Database helpers for executing read-only SQL via SQLAlchemy."""

from __future__ import annotations

import contextlib
from typing import Any, Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


_engine: Engine | None = None


def get_engine() -> Engine:
    """Return a singleton SQLAlchemy engine configured for read-only access."""
    global _engine
    if _engine is None:
        settings = get_settings().database
        logger.info("Initialising SQLAlchemy engine (pool_size=%s)", settings.pool_size)
        _engine = create_engine(
            settings.url,
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            future=True,
        )
    return _engine


def run_select(query: str, params: dict[str, Any] | None = None) -> Result:
    """Execute a read-only SQL query and return a SQLAlchemy ``Result``."""
    engine = get_engine()
    with engine.connect() as connection:
        return connection.execute(text(query), params or {})


@contextlib.contextmanager
def transactionless_connection():
    """Provide a connection without an explicit transaction for streaming."""
    engine = get_engine()
    with engine.connect() as connection:
        yield connection

