"""SQL guardrails enforcing read-only behaviour and safety checks."""

from __future__ import annotations

import re
from dataclasses import dataclass

_READ_ONLY_PATTERN = re.compile(r"^\s*(select|with)\b", re.IGNORECASE | re.DOTALL)
_PROHIBITED_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|create|grant|revoke|truncate|comment|merge|call|exec)\b",
    re.IGNORECASE,
)


class GuardrailViolation(Exception):
    """Raised when generated SQL violates safety policies."""


@dataclass
class SqlValidationResult:
    query: str
    enforced_limit: bool


def ensure_read_only(query: str) -> None:
    """Ensure the SQL query is read-only."""
    if not _READ_ONLY_PATTERN.match(query):
        raise GuardrailViolation("Only SELECT/CTE queries are permitted.")
    if _PROHIBITED_KEYWORDS.search(query):
        raise GuardrailViolation("Detected prohibited SQL keywords (DML/DDL).")
    if ";" in query.strip().rstrip(";"):
        raise GuardrailViolation("Multiple SQL statements are not allowed.")


def enforce_limit(query: str, limit: int) -> SqlValidationResult:
    """Ensure the query includes a LIMIT clause, appending if required."""
    # Simple detection: if LIMIT present outside subquery ending? We'll check final part.
    lower = query.lower()
    if " limit " in lower:
        return SqlValidationResult(query=query, enforced_limit=False)
    sanitized = query.rstrip().rstrip(";")
    enforced = f"{sanitized}\nLIMIT {limit}"
    return SqlValidationResult(query=enforced, enforced_limit=True)


def validate_sql(query: str, *, limit: int) -> SqlValidationResult:
    """Validate and normalise SQL query for execution."""
    ensure_read_only(query)
    return enforce_limit(query, limit)

