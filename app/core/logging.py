"""Logging configuration utilities."""

from __future__ import annotations

import logging
import json
import logging
import re
from contextvars import ContextVar, Token
from typing import Any, Dict, Optional


_REQUEST_ID_CTX: ContextVar[str] = ContextVar("request_id", default="n/a")
_SESSION_ID_CTX: ContextVar[str] = ContextVar("session_id", default="n/a")

_SENSITIVE_KEYS = {"ssn", "address", "passport", "drivers"}
_SENSITIVE_PATTERNS = [re.compile(r"\b\d{3}-\d{2}-\d{4}\b")]


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger with a concise format if not already configured."""
    root = logging.getLogger()
    if root.handlers:
        # Assume logging already configured by host (e.g., uvicorn).
        return

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)
    root.setLevel(level)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a module logger."""
    return logging.getLogger(name)


def set_request_id(request_id: str) -> Token:
    return _REQUEST_ID_CTX.set(request_id or "n/a")


def get_request_id() -> str:
    return _REQUEST_ID_CTX.get()


def reset_request_id(token: Token | None) -> None:
    if token is not None:
        _REQUEST_ID_CTX.reset(token)


def set_session_id(session_id: str) -> Token:
    return _SESSION_ID_CTX.set(session_id or "n/a")


def get_session_id() -> str:
    return _SESSION_ID_CTX.get()


def reset_session_id(token: Token | None) -> None:
    if token is not None:
        _SESSION_ID_CTX.reset(token)


def _redact_value(key: str, value: Any) -> Any:
    if value is None:
        return value
    lowered = key.lower()
    if any(token in lowered for token in _SENSITIVE_KEYS):
        return "***redacted***"
    if isinstance(value, str):
        for pattern in _SENSITIVE_PATTERNS:
            if pattern.search(value):
                return pattern.sub("***redacted***", value)
    return value


def redact_mapping(mapping: Dict[str, Any]) -> Dict[str, Any]:
    """Return a shallowly redacted copy of a mapping."""
    return {key: _redact_value(key, value) for key, value in mapping.items()}


def log_structured(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Emit a structured log entry with contextual metadata."""
    payload = redact_mapping(fields)
    payload.setdefault("request_id", get_request_id())
    payload.setdefault("session_id", get_session_id())
    logger.log(level, "%s | %s", message, json.dumps(payload, default=str, sort_keys=True))

