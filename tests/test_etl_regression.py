from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
import pandas.testing as pdt
import pytest
from sqlalchemy import create_engine

from app.core.config import ETLSettings, DatabaseSettings
from app.etl import json_to_s3
from app.agent import sql_executor
from app.core import db as core_db
from app.core import config as core_config


GOLDEN_DIR = Path(__file__).resolve().parent / "golden"
RAW_BUNDLE = GOLDEN_DIR / "raw" / "bundle.json"
ETL_EXPECTED_DIR = GOLDEN_DIR / "etl"
SQL_EXPECTED = GOLDEN_DIR / "sql" / "patient_first_names.json"


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    normalised = df.fillna("").astype(str).replace({"NaT": ""})
    # ensure stable column order for comparison
    return normalised.reindex(sorted(normalised.columns), axis=1).sort_values(list(sorted(normalised.columns))).reset_index(drop=True)


def _load_expected_dataframe(table: str) -> pd.DataFrame:
    path = ETL_EXPECTED_DIR / f"{table}.csv"
    expected = pd.read_csv(path, dtype=str).fillna("").astype(str)
    return expected.reindex(sorted(expected.columns), axis=1)


def test_etl_transform_matches_golden(tmp_path):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy(RAW_BUNDLE, raw_dir / "bundle.json")

    settings = ETLSettings(
        raw_dir=str(raw_dir),
        processed_dir=str(processed_dir),
        s3_bucket=None,
        s3_prefix="",
        aws_region=None,
        enable_s3=False,
        max_records=0,
        enable_db_load=False,
        manifest_path="",
    )

    file_paths = json_to_s3.extract(settings)
    assert len(file_paths) == 1

    datasets = json_to_s3.transform_all(file_paths, catalog=json_to_s3.get_schema_catalog(settings))

    expected_tables = {"patients", "encounters", "conditions", "medications", "observations", "procedures"}
    assert expected_tables.issubset(datasets.keys())

    for table in expected_tables:
        produced = _normalise(datasets[table])
        expected = _load_expected_dataframe(table)
        produced = produced.reindex(expected.columns, axis=1)
        pdt.assert_frame_equal(produced, expected, check_dtype=False)

    results = json_to_s3.run_pipeline_all(settings)
    for table in expected_tables:
        assert table in results
        csv_path = results[table]["local_path"]
        produced = pd.read_csv(csv_path, dtype=str).fillna("")
        expected = _load_expected_dataframe(table)
        produced = produced.reindex(expected.columns, axis=1)
        pdt.assert_frame_equal(produced, expected, check_dtype=False)


def test_sql_query_matches_golden(tmp_path, monkeypatch):
    db_path = tmp_path / "golden.db"
    engine = create_engine(f"sqlite:///{db_path}")

    patients_df = pd.read_csv(ETL_EXPECTED_DIR / "patients.csv", dtype=str).fillna("")
    patients_df.to_sql("patients", engine, index=False, if_exists="replace")

    db_settings = DatabaseSettings(url=f"sqlite:///{db_path}", pool_size=1, max_overflow=0)

    def fake_get_settings():
        return type(
            "Settings",
            (),
            {
                "default_result_limit": 10,
                "database": db_settings,
            },
        )()

    stub_settings = type(
        "Settings",
        (),
        {
            "default_result_limit": 10,
            "database": db_settings,
        },
    )()

    monkeypatch.setattr(sql_executor, "get_settings", lambda: stub_settings)
    monkeypatch.setattr(core_config, "get_settings", lambda: stub_settings)
    monkeypatch.setattr(core_db, "get_settings", lambda: stub_settings)
    monkeypatch.setattr(core_db, "_engine", None, raising=False)

    result = sql_executor.execute_query("SELECT id, first FROM patients ORDER BY id")

    expected_rows = json.loads(SQL_EXPECTED.read_text())
    assert result["rows"] == expected_rows
    assert result["columns"] == ["id", "first"]

