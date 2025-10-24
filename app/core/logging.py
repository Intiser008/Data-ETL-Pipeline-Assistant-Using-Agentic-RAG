"""Logging configuration utilities."""

from __future__ import annotations

import logging
from typing import Optional


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

