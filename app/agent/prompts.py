"""Prompt builders for the Lambda-hosted SQL generator."""

from __future__ import annotations

from typing import Sequence


def build_sql_prompt(
    user_prompt: str,
    context_chunks: Sequence[str],
    *,
    limit: int,
) -> str:
    """Render a single prompt string for the LLM proxy."""
    context = "\n\n".join(f"[Context #{idx + 1}]\n{chunk}" for idx, chunk in enumerate(context_chunks))
    instructions = (
        "You are an expert financial data analyst. "
        "Write a single read-only SQL query (SELECT or CTE) that answers the question below using the provided context. "
        "Rules: no DML/DDL; no table creation; list explicit column names; "
        f"include LIMIT {limit} or a smaller value; do not add commentary—return only the SQL."
    )
    prompt = (
        f"{instructions}\n\n"
        f"Context Documentation:\n{context}\n\n"
        f"User Question:\n{user_prompt.strip()}\n\n"
        "Return only the SQL query."
    )
    return prompt

