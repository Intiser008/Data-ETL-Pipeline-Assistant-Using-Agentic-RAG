
"""Agent service orchestrating planning, retrieval, LLM generation, and execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union

from sqlalchemy.exc import SQLAlchemyError

from app.agent.guardrails import GuardrailViolation, ensure_required_literals
from app.agent.llm import LambdaLLMClient, LLMError
from app.agent.planner import Intent, plan_intent
from app.agent.prompts import build_etl_prompt, build_sql_prompt, build_sql_repair_prompt
from app.agent.retriever import ChromaRetriever, RetrievalError
from app.agent.sql_executor import execute_query
from app.agent.validator import ValidationError, summarize_exception, validate_result
from app.core.config import get_settings
from app.core.logging import get_logger
from app.etl.db_loader import DBLoadError, LoadRequest, load_table_from_csv
from app.etl.json_to_s3 import ETLError, get_schema_catalog, run_pipeline_all
from app.core.cache import delete as cache_delete
from app.core.cache import get_client, get_json, set_json

logger = get_logger(__name__)


@dataclass
class SQLAgentResponse:
    sql: str
    rows: list[dict]
    columns: list[str]
    intent: Intent
    limit_enforced: bool
    attempts: int
    repaired: bool = False
    errors: list[str] = field(default_factory=list)


@dataclass
class ETLTableSummary:
    table: str
    row_count: int
    local_path: str
    s3_uri: str | None
    loaded_rows: int | None = None


@dataclass
class ETLAgentResponse:
    results: list[ETLTableSummary]
    intent: Intent
    attempts: int
    repaired: bool = False
    errors: list[str] = field(default_factory=list)


AgentResult = Union[SQLAgentResponse, ETLAgentResponse]


class AgentExecutionError(RuntimeError):
    """Raised when the agent exhausts all retries without success."""

    def __init__(self, message: str, *, attempts: int, errors: list[str]) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.errors = errors


class AgentService:
    """Main entrypoint for serving natural-language requests."""

    def __init__(self) -> None:
        self._llm = LambdaLLMClient()
        self._retriever = ChromaRetriever()
        self._max_retries = get_settings().agent_max_retries

    def handle_query(self, prompt: str) -> AgentResult:
        plan = plan_intent(prompt)
        if plan.intent is Intent.SQL:
            return self._handle_sql(prompt)
        if plan.intent is Intent.ETL:
            return self._handle_etl(prompt)
        raise NotImplementedError(f"Intent {plan.intent.name} not yet supported")

    # ------------------------------------------------------------------
    # SQL path
    # ------------------------------------------------------------------
    def _handle_sql(self, prompt: str) -> SQLAgentResponse:
        settings = get_settings()
        try:
            context = self._retriever.retrieve(prompt)
        except RetrievalError as exc:
            logger.exception("Retriever failed")
            raise

        limit = settings.default_result_limit
        error_messages: list[str] = []
        last_sql: str | None = None
        required_literals = _extract_required_literals(prompt)

        for attempt in range(1, self._max_retries + 1):
            if attempt == 1 or last_sql is None or not error_messages:
                sql_prompt = build_sql_prompt(prompt, context, limit=limit)
            else:
                sql_prompt = build_sql_repair_prompt(
                    user_prompt=prompt,
                    context_chunks=context,
                    previous_sql=last_sql,
                    error_summary=error_messages[-1],
                    limit=limit,
                )

            logger.info("Attempt %s generating SQL (repair=%s)", attempt, attempt > 1)

            try:
                sql = self._llm.generate(sql_prompt)
                if required_literals:
                    ensure_required_literals(sql, required_literals)
            except (LLMError, GuardrailViolation) as exc:
                summary = summarize_exception(exc)
                error_messages.append(summary.message)
                logger.warning("Attempt %s failed during LLM generation: %s", attempt, summary.message)
                last_sql = None
                if attempt == self._max_retries:
                    raise AgentExecutionError(
                        summary.message,
                        attempts=attempt,
                        errors=list(error_messages),
                    )
                continue

            try:
                result = execute_query(sql)
                validate_result(result["rows"])
                return SQLAgentResponse(
                    sql=result["sql"],
                    rows=result["rows"],
                    columns=result["columns"],
                    intent=Intent.SQL,
                    limit_enforced=result["limit_enforced"],
                    attempts=attempt,
                    repaired=attempt > 1,
                    errors=list(error_messages),
                )
            except (ValidationError, GuardrailViolation, SQLAlchemyError) as exc:
                summary = summarize_exception(exc)
                error_messages.append(summary.message)
                last_sql = sql
                logger.warning(
                    "Attempt %s failed during execution/validation: %s", attempt, summary.message
                )
                if attempt == self._max_retries:
                    raise AgentExecutionError(
                        summary.message,
                        attempts=attempt,
                        errors=list(error_messages),
                    )

        raise AgentExecutionError(
            "Agent failed after SQL retries.",
            attempts=self._max_retries,
            errors=list(error_messages),
        )

    # ------------------------------------------------------------------
    # ETL path
    # ------------------------------------------------------------------
    def _handle_etl(self, prompt: str) -> ETLAgentResponse:
        settings = get_settings()
        try:
            context = self._retriever.retrieve(prompt)
        except RetrievalError as exc:
            logger.exception("Retriever failed")
            raise

        error_history: list[str] = []
        cache_client = get_client()
        cache_key = None
        error_history_key: str | None = None
        skip_flag_key: str | None = None
        skip_tables: set[str] = set()
        if cache_client:
            cache_key = self._make_etl_cache_key(prompt, settings.etl)
            cached_payload = get_json(cache_key)
            if cached_payload:
                logger.info("ETL cache hit for key %s", cache_key)
                return _cached_response_to_etl_agent_response(cached_payload)
            error_history_key = f"{cache_key}:errors"
            cached_errors = get_json(error_history_key)
            if isinstance(cached_errors, list):
                error_history = [str(item) for item in cached_errors]
            skip_flag_key = f"{cache_key}:skip_tables"
            cached_skips = get_json(skip_flag_key)
            if isinstance(cached_skips, list):
                skip_tables = {str(item) for item in cached_skips}

        for attempt in range(1, self._max_retries + 1):
            etl_prompt = build_etl_prompt(
                prompt,
                context,
                error_history=error_history,
            )
            logger.info("Attempt %s generating ETL directive", attempt)

            try:
                directive_raw = self._llm.generate(etl_prompt)
                directive = self._parse_etl_directive(directive_raw)
                table_choice = directive["table"]
                if table_choice != "all":
                    logger.info("Processing full dataset; overriding directive '%s' to 'all'", table_choice)
                    table_choice = "all"
            except (LLMError, GuardrailViolation) as exc:
                summary = summarize_exception(exc)
                error_history.append(f"Attempt {attempt} directive failed: {summary.message}")
                if error_history_key:
                    set_json(error_history_key, error_history)
                logger.warning(
                    "Attempt %s failed during ETL directive generation: %s", attempt, summary.message
                )
                if attempt == self._max_retries:
                    raise AgentExecutionError(summary.message, attempts=attempt, errors=list(error_history))
                continue
            except ValueError as exc:
                summary = summarize_exception(exc)
                error_history.append(f"Attempt {attempt} directive invalid: {summary.message}")
                if error_history_key:
                    set_json(error_history_key, error_history)
                logger.warning("Attempt %s produced invalid ETL directive: %s", attempt, summary.message)
                if attempt == self._max_retries:
                    raise AgentExecutionError(summary.message, attempts=attempt, errors=list(error_history))
                continue

            try:
                pipeline_results = run_pipeline_all(settings.etl)
                if table_choice == "all":
                    tables = _order_tables(list(pipeline_results))
                else:
                    tables = _order_tables([table_choice])
                db_rows: dict[str, int | None] = {}
                if settings.etl.enable_db_load:
                    load_failure_summary = None
                    for table in tables:
                        if table in skip_tables:
                            logger.info(
                                "Skipping DB load for table %s due to cached duplicate key flag.",
                                table,
                            )
                            info_msg = f"DB load skipped for {table} (duplicate primary key detected earlier)."
                            if info_msg not in error_history:
                                error_history.append(info_msg)
                                if error_history_key:
                                    set_json(error_history_key, error_history)
                            db_rows[table] = None
                            continue
                        request = LoadRequest(
                            table=table,
                            csv_path=Path(pipeline_results[table]["local_path"]),
                            truncate_before_load=settings.etl.truncate_before_load,
                        )
                        try:
                            result = load_table_from_csv(
                                request,
                                database=settings.database,
                                chunksize=settings.etl.db_chunksize,
                            )
                            db_rows[result.table] = result.inserted_rows
                        except DBLoadError as exc:
                            message_lower = str(exc).lower()
                            if "duplicate key value violates unique constraint" in message_lower:
                                logger.info(
                                    "Duplicate key detected for table %s; recording skip flag.",
                                    table,
                                )
                                skip_tables.add(table)
                                if skip_flag_key:
                                    set_json(skip_flag_key, sorted(skip_tables))
                                info_msg = f"DB load skipped for {table} (duplicate primary key detected)."
                                if info_msg not in error_history:
                                    error_history.append(info_msg)
                                    if error_history_key:
                                        set_json(error_history_key, error_history)
                                db_rows[table] = None
                                continue
                            summary = summarize_exception(exc)
                            load_failure_summary = summary
                            error_history.append(f"Attempt {attempt} DB load failed: {summary.message}")
                            if error_history_key:
                                set_json(error_history_key, error_history)
                            logger.warning("Attempt %s DB load failed: %s", attempt, summary.message)
                            break

                    if load_failure_summary:
                        if attempt == self._max_retries:
                            raise AgentExecutionError(
                                load_failure_summary.message,
                                attempts=attempt,
                                errors=list(error_history),
                            )
                        continue

                summaries = [
                    ETLTableSummary(
                        table=table,
                        row_count=pipeline_results[table]["row_count"],
                        local_path=pipeline_results[table]["local_path"],
                        s3_uri=pipeline_results[table]["s3_uri"],
                        loaded_rows=db_rows.get(table),
                    )
                    for table in tables
                ]
                response = ETLAgentResponse(
                    results=summaries,
                    intent=Intent.ETL,
                    attempts=attempt,
                    repaired=attempt > 1,
                    errors=list(error_history),
                )
                if cache_key:
                    set_json(cache_key, _etl_agent_response_to_cache_payload(response))
                if error_history_key:
                    cache_delete(error_history_key)
                return response
            except ETLError as exc:
                summary = summarize_exception(exc)
                error_history.append(f"Attempt {attempt} pipeline failed: {summary.message}")
                logger.warning("Attempt %s ETL pipeline failed: %s", attempt, summary.message)
                if attempt == self._max_retries:
                    raise AgentExecutionError(summary.message, attempts=attempt, errors=list(error_history))

        raise AgentExecutionError(
            "Agent failed after ETL retries.",
            attempts=self._max_retries,
            errors=list(error_history),
        )

    @staticmethod
    def _parse_etl_directive(payload: str) -> dict[str, str]:
        cleaned = payload.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
            cleaned = cleaned.strip()
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].lstrip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            if "" in cleaned:
                cleaned = cleaned.split("", 1)[1].strip()

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ValueError("ETL directive is not valid JSON") from exc

        if not isinstance(data, dict) or "table" not in data:
            raise ValueError("ETL directive must be a JSON object with a 'table' field")

        table = str(data["table"]).strip().lower()
        catalog = get_schema_catalog(get_settings().etl)
        if table != "all" and table not in catalog.table_names:
            valid = ["all", *sorted(catalog.table_names)]
            raise ValueError(
                f"Table '{table}' is not supported. Choose from: {valid}"
            )
        return {"table": table}

    @staticmethod
    def _make_etl_cache_key(prompt: str, etl_settings) -> str:
        prompt_hash = hashlib.md5(prompt.strip().lower().encode("utf-8")).hexdigest()
        raw_dir = Path(etl_settings.raw_dir)
        entries: list[str] = []
        if raw_dir.exists():
            for path in sorted(raw_dir.glob("*.json")):
                try:
                    stat = path.stat()
                except OSError:
                    continue
                entries.append(f"{path.name}:{int(stat.st_mtime)}:{stat.st_size}")
        signature = "|".join(entries)
        dir_hash = hashlib.md5(signature.encode("utf-8")).hexdigest() if signature else "none"
        return f"etl:{prompt_hash}:{dir_hash}"


def _extract_required_literals(prompt: str) -> list[str]:
    """Extract literal tokens (dates, UUIDs) that must appear in generated SQL."""
    import re

    dates = re.findall(r"\d{4}-\d{2}-\d{2}", prompt)
    uuids = re.findall(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        prompt,
        flags=re.IGNORECASE,
    )
    return dates + uuids


def _etl_agent_response_to_cache_payload(response: ETLAgentResponse) -> dict[str, object]:
    return {
        "intent": response.intent.name,
        "attempts": response.attempts,
        "errors": response.errors,
        "results": [
            {
                "table": summary.table,
                "row_count": summary.row_count,
                "local_path": summary.local_path,
                "s3_uri": summary.s3_uri,
                "loaded_rows": summary.loaded_rows,
            }
            for summary in response.results
        ],
    }


def _cached_response_to_etl_agent_response(payload: dict[str, object]) -> ETLAgentResponse:
    results_payload = payload.get("results") or []
    summaries = [
        ETLTableSummary(
            table=item.get("table", ""),
            row_count=int(item.get("row_count", 0)),
            local_path=item.get("local_path", ""),
            s3_uri=item.get("s3_uri"),
            loaded_rows=item.get("loaded_rows"),
        )
        for item in results_payload
        if isinstance(item, dict)
    ]
    intent_name = payload.get("intent", "ETL")
    try:
        intent = Intent[intent_name]
    except (KeyError, TypeError):
        intent = Intent.ETL
    errors = payload.get("errors") or []
    return ETLAgentResponse(
        results=summaries,
        intent=intent,
        attempts=int(payload.get("attempts", 1)),
        repaired=False,
        errors=list(errors),
    )


def _order_tables(tables: list[str]) -> list[str]:
    ordered: list[str] = []
    seen = set()
    for table in PREFERRED_LOAD_ORDER:
        if table in tables:
            ordered.append(table)
            seen.add(table)
    for table in tables:
        if table not in seen:
            ordered.append(table)
    return ordered
PREFERRED_LOAD_ORDER = [
    "patients",
    "encounters",
    "procedures",
    "observations",
    "medications",
    "conditions",
]
