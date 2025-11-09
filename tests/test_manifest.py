from __future__ import annotations

import json
from pathlib import Path

from app.core.config import ETLSettings
from app.etl.manifest import load_manifest


def _base_settings(tmp_path: Path) -> ETLSettings:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    return ETLSettings(
        raw_dir=str(raw_dir),
        processed_dir=str(processed_dir),
        schema_config_path=None,
        s3_bucket="env-bucket",
        s3_prefix="raw/",
        enable_s3=True,
        enable_db_load=True,
        truncate_before_load=False,
        aws_region=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        max_records=0,
        db_chunksize=1000,
        source_pattern="*.json",
        manifest_path=None,
    )


def test_manifest_inherit_leaves_env_settings(tmp_path):
    base = _base_settings(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_payload = {
        "source": {"path": str(tmp_path / "raw"), "pattern": "*.json"},
        "transform": {},
        "target": {"enable_s3": "inherit"},
    }
    manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

    manifest = load_manifest(manifest_path)
    applied = manifest.apply(base)

    assert applied.enable_s3 is True
    assert applied.s3_bucket == "env-bucket"


def test_manifest_can_disable_s3(tmp_path):
    base = _base_settings(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_payload = {
        "source": {"path": str(tmp_path / "raw"), "pattern": "*.json"},
        "transform": {},
        "target": {"enable_s3": False},
    }
    manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

    manifest = load_manifest(manifest_path)
    applied = manifest.apply(base)

    assert applied.enable_s3 is False

