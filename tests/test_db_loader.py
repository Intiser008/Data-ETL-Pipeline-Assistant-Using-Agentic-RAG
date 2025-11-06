from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from app.core.config import DatabaseSettings
from app.etl.db_loader import DBLoadError, LoadRequest, load_table_from_csv


def test_load_table_from_csv_inserts_rows(tmp_path, monkeypatch):
    csv_path = tmp_path / "patients.csv"
    df = pd.DataFrame(
        [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]
    )
    df.to_csv(csv_path, index=False)

    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE patients (id TEXT, name TEXT)"))

    monkeypatch.setattr("app.core.db.get_engine", lambda: engine)

    result = load_table_from_csv(
        LoadRequest(table="patients", csv_path=csv_path),
        database=DatabaseSettings(url="sqlite://"),
    )

    assert result.inserted_rows == 2
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT COUNT(*) FROM patients")).scalar()
    assert rows == 2


def test_load_table_from_csv_missing_file(tmp_path, monkeypatch):
    engine = create_engine("sqlite://")
    monkeypatch.setattr("app.core.db.get_engine", lambda: engine)
    with pytest.raises(DBLoadError):
        load_table_from_csv(
            LoadRequest(table="patients", csv_path=tmp_path / "missing.csv"),
            database=DatabaseSettings(url="sqlite://"),
        )
