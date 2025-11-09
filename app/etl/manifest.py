"""Utilities for loading ETL task manifests and applying overrides."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Tuple

from app.core.config import ETLSettings
from app.core.logging import get_logger

logger = get_logger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST_PATH = ROOT_DIR / "config" / "etl_manifest.json"


def _resolve_path(path_like: str | Path) -> Path:
    """Return an absolute path for the supplied value."""

    path = Path(path_like)
    if not path.is_absolute():
        path = (ROOT_DIR / path).resolve()
    return path


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(value)


def _as_int(value: Any, *, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


_INHERIT_SENTINELS = {"auto", "env", "inherit"}


def _should_override(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and value.strip().lower() in _INHERIT_SENTINELS:
        return False
    return True


def _normalise_path_value(value: Any) -> str | None:
    if value in (None, ""):
        return None
    try:
        return str(_resolve_path(value))
    except OSError:
        return str(value)


@dataclass(frozen=True)
class ETLManifest:
    """Represents a task manifest used to orchestrate ETL runs."""

    path: Path
    source: dict[str, Any]
    transform: dict[str, Any]
    target: dict[str, Any]

    def apply(self, base: ETLSettings) -> ETLSettings:
        """Return ETL settings with manifest overrides applied."""

        overrides: dict[str, Any] = {"manifest_path": str(self.path)}

        source = self.source or {}
        source_path = (
            source.get("path")
            or source.get("directory")
            or source.get("local_path")
        )
        if source_path:
            normalised = _normalise_path_value(source_path)
            if normalised:
                overrides["raw_dir"] = normalised

        pattern = source.get("pattern") or source.get("glob")
        if pattern:
            overrides["source_pattern"] = str(pattern)

        transform = self.transform or {}
        schema_path = (
            transform.get("schema_config")
            or transform.get("schema")
            or transform.get("mappings")
            or transform.get("catalog")
        )
        if schema_path:
            normalised = _normalise_path_value(schema_path)
            if normalised:
                overrides["schema_config_path"] = normalised

        max_records = transform.get("max_records")
        if max_records is not None:
            overrides["max_records"] = _as_int(max_records, default=base.max_records)

        target = self.target or {}
        processed_dir = (
            target.get("processed_dir")
            or target.get("directory")
            or target.get("path")
        )
        if processed_dir:
            normalised = _normalise_path_value(processed_dir)
            if normalised:
                overrides["processed_dir"] = normalised

        bucket_raw = target.get("s3_bucket")
        if bucket_raw is None and "bucket" in target:
            bucket_raw = target.get("bucket")
        bucket_override_applied = False
        if _should_override(bucket_raw):
            overrides["s3_bucket"] = str(bucket_raw) if bucket_raw else None
            bucket_override_applied = True
        bucket_value_for_default = overrides.get("s3_bucket") if bucket_override_applied else None

        prefix = target.get("s3_prefix") or target.get("prefix")
        if prefix is not None:
            overrides["s3_prefix"] = str(prefix)

        if "enable_s3" in target:
            enable_value = target.get("enable_s3")
            if _should_override(enable_value):
                overrides["enable_s3"] = _as_bool(enable_value)
        elif bucket_override_applied:
            overrides["enable_s3"] = bool(bucket_value_for_default)

        if "enable_db_load" in target:
            overrides["enable_db_load"] = _as_bool(target.get("enable_db_load"))

        truncate = target.get("truncate_before_load") or target.get("truncate")
        if truncate is not None:
            overrides["truncate_before_load"] = _as_bool(truncate)

        if "db_chunksize" in target or "chunksize" in target:
            overrides["db_chunksize"] = _as_int(
                target.get("db_chunksize", target.get("chunksize")),
                default=base.db_chunksize,
            )

        return replace(base, **overrides)


def _load_manifest_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest root must be a JSON object")
    return payload


def load_manifest(path: Path) -> ETLManifest:
    payload = _load_manifest_payload(path)
    source = payload.get("source") or {}
    transform = payload.get("transform") or {}
    target = payload.get("target") or {}
    if not isinstance(source, dict):  # pragma: no cover - configuration error
        raise ValueError("Manifest 'source' section must be an object")
    if not isinstance(transform, dict):  # pragma: no cover - configuration error
        raise ValueError("Manifest 'transform' section must be an object")
    if not isinstance(target, dict):  # pragma: no cover - configuration error
        raise ValueError("Manifest 'target' section must be an object")
    return ETLManifest(path=path, source=source, transform=transform, target=target)


def resolve_etl_settings(base: ETLSettings) -> Tuple[ETLSettings, ETLManifest | None]:
    """Return ETL settings optionally overridden by a manifest."""

    manifest_path = base.manifest_path
    candidate: Path | None = None

    if manifest_path is not None:
        if manifest_path == "":
            return base, None
        candidate = _resolve_path(manifest_path)
    elif DEFAULT_MANIFEST_PATH.exists():
        candidate = DEFAULT_MANIFEST_PATH

    if candidate is None or not candidate.exists():
        return base, None

    try:
        manifest = load_manifest(candidate)
    except Exception as exc:  # pragma: no cover - surface manifest issues
        logger.warning("Failed to load ETL manifest %s: %s", candidate, exc)
        return base, None

    applied = manifest.apply(base)
    logger.info("Loaded ETL manifest from %s", candidate)
    return applied, manifest

