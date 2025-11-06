"""Validation helpers and error summaries for the SQL agent retry loop."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from sqlalchemy.exc import SQLAlchemyError

from app.agent.guardrails import GuardrailViolation


class ValidationError(Exception):
    """Raised when post-execution validation fails."""


@dataclass(frozen=True)
class ValidationSummary:
    message: str
    details: dict[str, str] | None = None


def validate_result(rows: Sequence[dict]) -> None:
    """Ensure the SQL result set passes baseline sanity checks."""
    if not rows:
        raise ValidationError("Query returned no rows; relax filters or adjust the time window.")


def summarize_exception(exc: Exception) -> ValidationSummary:
    """Convert exceptions into short, LLM-friendly messages."""
    if isinstance(exc, ValidationError):
        return ValidationSummary(message=str(exc))
    if isinstance(exc, GuardrailViolation):
        return ValidationSummary(message=f"Guardrail violation: {exc}")
    if isinstance(exc, SQLAlchemyError):
        text = str(exc.__cause__ or exc)
        return ValidationSummary(message=f"Database error: {text}")
    return ValidationSummary(message=str(exc))

