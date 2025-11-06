"""Schema-related helpers shared between ETL routines."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype

TABLE_DATE_COLUMNS: Dict[str, List[str]] = {
    "patients": ["birthdate", "deathdate"],
    "encounters": ["date"],
    "conditions": ["start", "stop"],
    "observations": ["date"],
    "medications": ["start", "stop"],
    "procedures": ["date"],
}

TABLE_UUID_COLUMNS: Dict[str, List[str]] = {
    "patients": ["id"],
    "encounters": ["id", "patient"],
    "conditions": ["patient", "encounter"],
    "observations": ["patient", "encounter"],
    "medications": ["patient", "encounter"],
    "procedures": ["patient", "encounter"],
}

TABLE_DATE_COLUMNS: Dict[str, List[str]] = {
    "patients": ["birthdate", "deathdate"],
    "encounters": ["date"],
    "conditions": ["start", "stop"],
    "observations": ["date"],
    "medications": ["start", "stop"],
    "procedures": ["date"],
}


def normalize_date_columns(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Convert timestamp-like strings to dates for known columns."""
    columns = TABLE_DATE_COLUMNS.get(table, [])
    if not columns:
        return df
    converted = df.copy()
    for column in columns:
        if column not in converted.columns:
            continue
        parsed = pd.to_datetime(converted[column], errors="coerce", utc=True)
        if not is_datetime64_any_dtype(parsed.dtype):
            # Leave column as-is when it cannot be parsed as datetime.
            converted[column] = parsed
            continue
        if is_datetime64tz_dtype(parsed.dtype):
            parsed = parsed.dt.tz_convert(None)
        converted[column] = parsed.dt.date
    return converted


def normalize_uuid_columns(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Convert UUID-like strings to actual UUID objects where possible."""
    import uuid

    columns = TABLE_UUID_COLUMNS.get(table, [])
    if not columns:
        return df

    converted = df.copy()
    for column in columns:
        if column not in converted.columns:
            continue

        def _to_uuid(value):
            if value is None or (isinstance(value, str) and not value.strip()):
                return None
            if pd.isna(value):
                return None
            try:
                return uuid.UUID(str(value))
            except (ValueError, TypeError):
                return None

        converted[column] = converted[column].apply(_to_uuid)
    return converted
