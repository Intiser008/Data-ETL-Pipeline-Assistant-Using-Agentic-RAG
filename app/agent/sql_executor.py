"""SQL execution pipeline using guardrails and SQLAlchemy."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

from sqlalchemy.engine import Result

from app.agent.guardrails import SqlValidationResult, validate_sql
from app.core.config import get_settings
from app.core.db import run_select
from app.core.logging import get_logger

logger = get_logger(__name__)


def execute_query(query: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Validate and execute SQL, returning rows and metadata."""
    limit = get_settings().default_result_limit
    validation: SqlValidationResult = validate_sql(query, limit=limit)
    logger.info("Executing SQL query (limit_enforced=%s)", validation.enforced_limit)
    result = run_select(validation.query, params)
    rows = _result_to_dicts(result)
    return {
        "rows": rows,
        "columns": result.keys(),
        "sql": validation.query,
        "limit_enforced": validation.enforced_limit,
    }


def _result_to_dicts(result: Result) -> List[Dict[str, Any]]:
    cursor_metadata = result.keys()
    return [dict(zip(cursor_metadata, row)) for row in result.fetchall()]

