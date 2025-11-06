"""Simple intent planner deciding which toolchain to invoke."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class Intent(Enum):
    SQL = auto()
    ETL = auto()
    CHART = auto()


@dataclass(frozen=True)
class Plan:
    intent: Intent


SQL_KEYWORDS = {
    "show",
    "list",
    "average",
    "sum",
    "count",
    "total",
    "table",
    "query",
    "select",
}

ETL_KEYWORDS = {
    "load",
    "ingest",
    "transform",
    "cleanup",
    "clean",
    "normalize",
    "standardize",
    "aggregate",
    "group",
    "groupby",
    "summarize",
    "enrich",
    "csv",
    "excel",
    "xlsx",
    "parquet",
    "dataset",
    "pandas",
    "json",
    "jsonl",
    "s3",
    "upload",
    "pipeline",
    "etl",
    "stage",
    "prepare",
}
CHART_KEYWORDS = {"chart", "plot", "graph", "visualize", "visualise", "visualization", "dashboard", "plotly"}


def plan_intent(prompt: str) -> Plan:
    """Very lightweight keyword-based intent classifier."""
    lowered = prompt.lower()
    if any(word in lowered for word in CHART_KEYWORDS):
        return Plan(intent=Intent.CHART)
    if any(word in lowered for word in ETL_KEYWORDS):
        return Plan(intent=Intent.ETL)
    # Default to SQL path
    if any(word in lowered for word in SQL_KEYWORDS):
        return Plan(intent=Intent.SQL)
    # Fallback: assume SQL but future iteration can call LLM
    return Plan(intent=Intent.SQL)

