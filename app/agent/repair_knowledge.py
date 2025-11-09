"""Persistence helpers for remembering ETL load repair strategies."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from app.core.logging import get_logger

logger = get_logger(__name__)

KNOWLEDGE_PATH = Path(".cache/etl_repair_knowledge.json")


class RepairKnowledge:
    """Stores per-table strategies that resolved previous ETL load failures."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or KNOWLEDGE_PATH
        self._data: Dict[str, Dict[str, object]] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:  # pragma: no cover - corrupted cache
            logger.warning("Repair knowledge file is invalid JSON: %s", exc)
            return
        tables = payload.get("tables") if isinstance(payload, dict) else None
        if isinstance(tables, dict):
            for table, entry in tables.items():
                if isinstance(entry, dict) and isinstance(table, str):
                    self._data[table] = dict(entry)

    def _save(self) -> None:
        payload = {"tables": self._data}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def get_strategy(self, table: str) -> str | None:
        entry = self._data.get(table)
        if not isinstance(entry, dict):
            return None
        strategy = entry.get("strategy")
        return str(strategy) if isinstance(strategy, str) else None

    def record_strategy(self, table: str, strategy: str, error: str | None = None) -> None:
        self._data[table] = {
            "strategy": strategy,
            "last_error": error,
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        self._save()

    def clear_strategy(self, table: str) -> None:
        if table in self._data:
            del self._data[table]
            self._save()

