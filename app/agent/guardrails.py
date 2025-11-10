"""SQL guardrails enforcing read-only behaviour and safety checks."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable, Set, List

from sqlglot import exp, parse_one

logger = logging.getLogger(__name__)

_READ_ONLY_PATTERN = re.compile(r"^\s*(select|with)\b", re.IGNORECASE | re.DOTALL)
_PROHIBITED_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|alter|create|grant|revoke|truncate|comment|merge|call|exec)\b",
    re.IGNORECASE,
)

_TABLE_COLUMN_MAP: dict[str, set[str]] = {
    "patients": {
        "id",
        "birthdate",
        "deathdate",
        "ssn",
        "drivers",
        "passport",
        "prefix",
        "first",
        "last",
        "suffix",
        "maiden",
        "marital",
        "race",
        "ethnicity",
        "gender",
        "birthplace",
        "address",
    },
    "encounters": {
        "id",
        "date",
        "patient",
        "code",
        "description",
        "reasoncode",
        "reasondescription",
    },
    "conditions": {
        "start",
        "stop",
        "patient",
        "encounter",
        "code",
        "description",
    },
    "procedures": {
        "date",
        "patient",
        "encounter",
        "code",
        "description",
        "reasoncode",
        "reasondescription",
    },
    "medications": {
        "start",
        "stop",
        "patient",
        "encounter",
        "code",
        "description",
        "reasoncode",
        "reasondescription",
    },
    "observations": {
        "date",
        "patient",
        "encounter",
        "code",
        "description",
        "value",
        "units",
    },
}
_ALLOWED_COLUMN_NAMES: Set[str] = {col for cols in _TABLE_COLUMN_MAP.values() for col in cols}
_ALLOWED_TABLE_NAMES: Set[str] = set(_TABLE_COLUMN_MAP.keys())

_PROMPT_PROHIBITED_KEYWORDS = {
    "drop",
    "delete",
    "truncate",
    "alter",
    "update",
    "insert",
    "create",
    "grant",
    "revoke",
}


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


def ensure_known_columns(query: str) -> None:
    """Ensure generated SQL does not reference unknown column names."""
    try:
        tree = parse_one(query, read="postgres")
    except Exception as exc:  # pragma: no cover - parser failures bubble up
        logger.warning("Skipping column validation for unparsable SQL: %s", exc)
        return

    for column in tree.find_all(exp.Column):
        select_ancestor = column.find_ancestor(exp.Select)
        alias_names = _collect_aliases(select_ancestor) if select_ancestor else set()
        _validate_column_reference(column, alias_names)


def ensure_known_tables(query: str) -> None:
    """Ensure the query references only documented base tables."""
    try:
        tree = parse_one(query, read="postgres")
    except Exception as exc:  # pragma: no cover - parser failures bubble up
        logger.warning("Skipping table validation for unparsable SQL: %s", exc)
        return

    unknown: List[str] = []
    for table in tree.find_all(exp.Table):
        name = getattr(table, "name", None)
        if not name:
            continue
        normalized = str(name).strip('"').lower()
        # schema-qualified names like healthcare_demo.patients -> 'patients'
        if "." in normalized:
            normalized = normalized.split(".")[-1]
        if normalized not in _ALLOWED_TABLE_NAMES:
            unknown.append(str(name))
    if unknown:
        raise GuardrailViolation(
            f"Unknown table(s): {', '.join(sorted(unknown))}. "
            f"Use only documented tables: {', '.join(sorted(_ALLOWED_TABLE_NAMES))}."
        )


def _collect_aliases(select: exp.Select | None) -> Set[str]:
    if select is None:
        return set()
    aliases: Set[str] = set()
    for expression in select.expressions:
        alias = expression.alias
        if alias:
            alias_name = alias if isinstance(alias, str) else getattr(alias, "name", str(alias))
            if alias_name:
                aliases.add(alias_name.lower())
    return aliases


def _validate_column_reference(column: exp.Column, alias_names: Set[str]) -> None:
    """Validate that a column reference maps to a known physical column."""
    if column.is_star:
        return

    name = column.name
    if not name:
        return

    normalized = name.strip('"').lower()

    if normalized in alias_names:
        return

    if normalized not in _ALLOWED_COLUMN_NAMES:
        raise GuardrailViolation(
            f"Unknown column '{normalized}'. Valid columns include: {sorted(_ALLOWED_COLUMN_NAMES)}"
        )


def enforce_limit(query: str, limit: int) -> SqlValidationResult:
    """Ensure the query includes a LIMIT clause, appending if required."""
    if re.search(r"\blimit\b", query, re.IGNORECASE):
        return SqlValidationResult(query=query, enforced_limit=False)
    sanitized = query.rstrip().rstrip(";")
    enforced = f"{sanitized}\nLIMIT {limit}"
    return SqlValidationResult(query=enforced, enforced_limit=True)


def validate_sql(query: str, *, limit: int) -> SqlValidationResult:
    """Validate and normalise SQL query for execution."""
    ensure_read_only(query)
    ensure_known_tables(query)
    ensure_known_columns(query)
    return enforce_limit(query, limit)


def ensure_safe_prompt(prompt: str) -> None:
    """Basic prompt-level guardrail to stop direct DDL/DML instructions."""
    lowered = prompt.lower()
    if any(keyword in lowered for keyword in _PROMPT_PROHIBITED_KEYWORDS):
        raise GuardrailViolation(
            "Detected potentially destructive instruction in the prompt. "
            "Only read-only analytics requests are permitted."
        )


def allowed_table_names() -> Set[str]:
    """Return the set of allowed base table names."""
    return set(_ALLOWED_TABLE_NAMES)


def ensure_prompt_tables_known(prompt: str) -> None:
    """Best-effort scan of the prompt for table names like 'from <name>' or 'join <name>'.
    If any are not in the allowed table set, raise a violation early with a helpful message.
    """
    lowered = prompt.lower()
    candidates: Set[str] = set()
    # naive extraction after keywords
    for kw in (" from ", " join ", " table "):
        idx = 0
        while True:
            idx = lowered.find(kw, idx)
            if idx == -1:
                break
            start = idx + len(kw)
            end = start
            while end < len(lowered) and (lowered[end].isalnum() or lowered[end] in {"_", ".", '"'}):
                end += 1
            token = lowered[start:end].strip().strip('"')
            if token:
                # schema-qualified -> take last part
                if "." in token:
                    token = token.split(".")[-1]
                candidates.add(token)
            idx = end
    unknown = [t for t in candidates if t and t not in _ALLOWED_TABLE_NAMES]
    if unknown:
        raise GuardrailViolation(
            f"Detected unknown table name(s) in the request: {', '.join(sorted(unknown))}. "
            f"Valid tables: {', '.join(sorted(_ALLOWED_TABLE_NAMES))}."
        )


def ensure_required_literals(query: str, literals: Iterable[str]) -> None:
    """Ensure all required literal strings appear in the generated SQL."""
    lowered = query.lower()
    missing = [literal for literal in literals if literal.lower() not in lowered]
    if missing:
        raise GuardrailViolation(
            f"Generated SQL omitted required values: {', '.join(missing)}"
        )


