"""Utilities for loading curated CSV datasets into relational databases."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import DatabaseSettings
from app.core.db import get_engine
from app.core.logging import get_logger
from app.etl.schema_utils import (
    TABLE_UUID_COLUMNS,
    normalize_date_columns,
    normalize_uuid_columns,
)

logger = get_logger(__name__)


class DBLoadError(RuntimeError):
    """Raised when staging data into the database fails."""


@dataclass(frozen=True)
class LoadRequest:
    table: str
    csv_path: Path
    truncate_before_load: bool = False


@dataclass(frozen=True)
class LoadResult:
    table: str
    inserted_rows: int
    source_path: Path


def load_table_from_csv(
    request: LoadRequest,
    *,
    database: DatabaseSettings,
    chunksize: int = 1000,
) -> LoadResult:
    """Load a single CSV file into the configured database table."""

    csv_path = request.csv_path
    if not csv_path.exists():
        raise DBLoadError(f"Source CSV not found: {csv_path}")

    # Avoid leaking credentials when logging target.
    target_db = database.url.split("@")[-1] if "@" in database.url else database.url
    logger.info("Target database: %s", target_db)

    engine = get_engine()
    logger.info("Loading CSV into table %s from %s", request.table, csv_path)

    try:
        df = pd.read_csv(csv_path)
        df = normalize_date_columns(df, request.table)
        df = normalize_uuid_columns(df, request.table)
    except Exception as exc:  # pragma: no cover - pandas error surface
        raise DBLoadError(f"Failed to read CSV {csv_path}: {exc}") from exc

    inserted_rows = int(df.shape[0])
    if inserted_rows == 0:
        logger.info("CSV %s is empty; skipping load for table %s", csv_path, request.table)
        return LoadResult(table=request.table, inserted_rows=0, source_path=csv_path)

    try:
        dtype_map = None
        try:
            url = make_url(database.url)
        except Exception:  # pragma: no cover - safeguard malformed URLs
            url = None

        if url and url.get_backend_name().startswith("postgresql"):
            from sqlalchemy.dialects.postgresql import UUID as PG_UUID  # type: ignore

            uuid_columns = TABLE_UUID_COLUMNS.get(request.table, [])
            if uuid_columns:
                dtype_map = {column: PG_UUID(as_uuid=True) for column in uuid_columns}

        with engine.begin() as connection:
            if request.truncate_before_load:
                logger.info("Truncating table %s before load", request.table)
                connection.execute(text(f'TRUNCATE TABLE "{request.table}"'))
            df.to_sql(
                request.table,
                connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=chunksize,
                dtype=dtype_map,
            )
    except SQLAlchemyError as exc:
        raise DBLoadError(f"Database load failed for table {request.table}: {exc}") from exc

    logger.info("Inserted %s rows into table %s", inserted_rows, request.table)
    return LoadResult(table=request.table, inserted_rows=inserted_rows, source_path=csv_path)


def load_tables(
    requests: Iterable[LoadRequest],
    *,
    database: DatabaseSettings,
    chunksize: int = 1000,
) -> list[LoadResult]:
    """Load multiple CSV files into the database."""
    results: list[LoadResult] = []
    for request in requests:
        results.append(
            load_table_from_csv(request, database=database, chunksize=chunksize)
        )
    return results
