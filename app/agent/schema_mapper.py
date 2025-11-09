"""Schema mapping utilities powered by the LLM with local caching."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, Mapping, MutableMapping, Sequence

from app.agent.prompts import build_schema_mapping_prompt
from app.agent.validator import summarize_exception
from app.core.logging import get_logger
from app.etl.manifest import ETLManifest
from app.etl.schema_catalog import SchemaCatalog

logger = get_logger(__name__)

CACHE_PATH = Path(".cache/etl_cache.json")


class SchemaMappingError(RuntimeError):
    """Raised when schema mapping generation fails."""


class SchemaMappingCache:
    """Simple JSON-backed cache for schema mapping results."""

    def __init__(self, path: Path | None = None) -> None:
        self._path = path or CACHE_PATH
        self._data: MutableMapping[str, Dict[str, str]] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:  # pragma: no cover - corrupted cache
            logger.warning("Schema mapping cache is invalid JSON: %s", exc)
            return
        if isinstance(payload, dict):
            self._data.update(
                {
                    str(key): {str(k): str(v) for k, v in value.items() if isinstance(v, (str, type(None)))}
                    for key, value in payload.items()
                    if isinstance(value, dict)
                }
            )

    def get(self, key: str) -> Dict[str, str] | None:
        value = self._data.get(key)
        if value is None:
            return None
        return dict(value)

    def set(self, key: str, mapping: Mapping[str, str]) -> None:
        self._data[key] = dict(mapping)
        self._save()

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._data, indent=2, sort_keys=True), encoding="utf-8")


@dataclass(frozen=True)
class GeneratedMapping:
    table: str
    columns: Dict[str, str]


class SchemaMapper:
    """Generate mappings between source columns and catalogued target schemas."""

    def __init__(
        self,
        *,
        generate_fn: Callable[[str], str],
        cache: SchemaMappingCache | None = None,
    ) -> None:
        self._generate = generate_fn
        self._cache = cache or SchemaMappingCache()

    def generate_mappings(
        self,
        tables: Sequence[str],
        *,
        catalog: SchemaCatalog,
        source_hints: Mapping[str, Sequence[str]] | None = None,
        manifest: ETLManifest | None = None,
        namespace: str | None = None,
    ) -> Dict[str, Dict[str, str]]:
        mappings: Dict[str, Dict[str, str]] = {}
        for table in tables:
            target_columns = catalog.get_columns(table)
            sources = None
            if source_hints:
                sources = source_hints.get(table)
            if not sources:
                sources = target_columns
            try:
                mapping = self._generate_for_table(
                    table,
                    source_columns=list(sources),
                    target_columns=target_columns,
                    manifest=manifest,
                    namespace=namespace,
                )
            except SchemaMappingError as exc:
                logger.warning("Schema mapping failed for table %s: %s", table, exc)
                raise
            if mapping:
                mappings[table] = mapping
        return mappings

    def _generate_for_table(
        self,
        table: str,
        *,
        source_columns: Sequence[str],
        target_columns: Sequence[str],
        manifest: ETLManifest | None,
        namespace: str | None,
    ) -> Dict[str, str]:
        cache_key = self._build_cache_key(table, source_columns, target_columns, namespace)
        cached = self._cache.get(cache_key)
        if cached:
            return cached

        prompt = build_schema_mapping_prompt(
            table_name=table,
            source_columns=source_columns,
            target_columns=target_columns,
            manifest_transform=manifest.transform if manifest else None,
        )

        try:
            response = self._generate(prompt)
        except Exception as exc:  # pragma: no cover - LLM runtime wiring
            summary = summarize_exception(exc)
            raise SchemaMappingError(summary.message) from exc

        mapping = self._parse_response(response, target_columns)
        self._cache.set(cache_key, mapping)
        return mapping

    @staticmethod
    def _build_cache_key(
        table: str,
        source_columns: Sequence[str],
        target_columns: Sequence[str],
        namespace: str | None,
    ) -> str:
        payload = json.dumps(
            {
                "table": table,
                "source": sorted(str(column) for column in source_columns),
                "target": list(target_columns),
                "namespace": namespace or "default",
            },
            sort_keys=True,
        )
        digest = hashlib.md5(payload.encode("utf-8")).hexdigest()
        return f"{table}:{digest}"

    @staticmethod
    def _strip_code_fence(text: str) -> str:
        cleaned = text.strip()
        if cleaned.startswith("```") and cleaned.endswith("```"):
            cleaned = cleaned[3:-3].strip()
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].strip()
        return cleaned

    def _parse_response(self, payload: str, target_columns: Sequence[str]) -> Dict[str, str]:
        cleaned = self._strip_code_fence(payload)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise SchemaMappingError(f"LLM returned invalid JSON: {exc}") from exc

        if isinstance(data, dict) and isinstance(data.get("columns"), dict):
            columns = data["columns"]
        elif isinstance(data, dict):
            columns = data
        else:
            raise SchemaMappingError("LLM response must be a JSON object with column mappings")

        mapping: Dict[str, str] = {}
        for column in target_columns:
            raw_value = columns.get(column, columns.get(column.upper())) if isinstance(columns, dict) else None
            if raw_value is None:
                mapping[column] = column
                continue
            if not isinstance(raw_value, str) or not raw_value.strip():
                mapping[column] = column
                continue
            mapping[column] = raw_value.strip()
        return mapping

