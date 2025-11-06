"""Prompt builders for the Lambda-hosted SQL and ETL generators."""

from __future__ import annotations

from typing import Sequence


def build_sql_prompt(
    user_prompt: str,
    context_chunks: Sequence[str],
    *,
    limit: int,
) -> str:
    """Render a single prompt string for the SQL generation proxy."""
    context = "\n\n".join(f"[Context #{idx + 1}]\n{chunk}" for idx, chunk in enumerate(context_chunks))
    instructions = (
        "You are an expert healthcare data analyst. "
        "Write a single read-only SQL query (SELECT or CTE) that answers the question below using the provided context. "
        "Use only the tables and columns documented, prefer schema-qualified names such as healthcare_demo.table_name when in doubt, "
        "and do not fabricate columns or tables. "
        f"Rules: no DML/DDL; no table creation; list explicit column names; include LIMIT {limit} or a smaller value; "
        "do not add commentary—return only the SQL."
    )
    return (
        f"{instructions}\n\n"
        f"Context Documentation:\n{context}\n\n"
        f"User Question:\n{user_prompt.strip()}\n\n"
        "Return only the SQL query."
    )


def build_sql_repair_prompt(
    user_prompt: str,
    context_chunks: Sequence[str],
    previous_sql: str,
    error_summary: str,
    *,
    limit: int,
) -> str:
    """Prompt variant guiding the LLM to repair a failing SQL query."""
    context = "\n\n".join(f"[Context #{idx + 1}]\n{chunk}" for idx, chunk in enumerate(context_chunks))
    instructions = (
        "You previously generated a SQL query that failed during execution. "
        "Using the same documentation, produce a corrected SQL query that fixes the issue described below. "
        "Stick strictly to documented healthcare_demo tables and columns; do not invent new fields. "
        f"Rules: write a single read-only SQL statement (SELECT or CTE); avoid DML/DDL; include LIMIT {limit} or a smaller value; "
        "ensure column names exactly match the schema; return only the SQL."
    )
    return (
        f"{instructions}\n\n"
        f"Context Documentation:\n{context}\n\n"
        f"User Question:\n{user_prompt.strip()}\n\n"
        f"Previous SQL:\n{previous_sql.strip()}\n\n"
        f"Execution Error:\n{error_summary.strip()}\n\n"
        "Return only the corrected SQL query."
    )


def build_etl_prompt(
    user_prompt: str,
    context_chunks: Sequence[str],
    *,
    error_history: Sequence[str] | None = None,
) -> str:
    """Prompt to obtain ETL directives (e.g., which table to process)."""
    context = "\n\n".join(f"[Context #{idx + 1}]\n{chunk}" for idx, chunk in enumerate(context_chunks))
    instructions = (
        "You are an ETL specialist. Interpret the user request and select the appropriate target table "
        "from the documented healthcare datasets (patients, encounters, conditions, observations, medications, procedures). "
        "If the user wants the entire pipeline, respond with {\"table\": \"all\"}; otherwise pick the most relevant single table. "
        "Respond with a compact JSON object matching the schema {\"table\": \"<table_name>\"}. "
        "Only use lowercase singular table names from the list (or 'all'); do not fabricate new tables."
    )
    if error_history:
        history = "\n".join(f"- {entry.strip()}" for entry in error_history if entry.strip())
        if history:
            instructions += (
                "\nPrevious attempts failed:\n"
                f"{history}\n"
                "Adjust your directive to avoid repeating the same error."
            )
    return (
        f"{instructions}\n\n"
        f"Context Documentation:\n{context}\n\n"
        f"User Request:\n{user_prompt.strip()}\n\n"
        "Return ONLY the JSON object (no explanatory text)."
    )
