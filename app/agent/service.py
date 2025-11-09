
"""Agent service orchestrating planning, retrieval, LLM generation, and execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid
import time
import logging

from sqlalchemy.exc import SQLAlchemyError

from app.agent.guardrails import GuardrailViolation, ensure_required_literals, ensure_safe_prompt
from app.agent.llm import LambdaLLMClient, LLMError
from app.agent.planner import Intent, plan_intent
from app.agent.prompts import build_etl_prompt, build_sql_prompt, build_sql_repair_prompt
from app.agent.repair_knowledge import RepairKnowledge
from app.agent.retriever import ChromaRetriever, RetrievalError
from app.agent.sql_executor import execute_query
from app.agent.validator import ValidationError, summarize_exception, validate_result
from app.agent.schema_mapper import SchemaMapper, SchemaMappingError
from app.agent.intent_classifier import IntentClassifier
from app.agent.conversation_store import (
    append_agent_turn,
    append_user_turn,
    get_history as conversation_history,
    get_last_intent,
    set_last_intent,
)
from app.core.config import ETLSettings, get_settings
from app.core.logging import get_logger, log_structured, reset_session_id, set_session_id
from app.etl.db_loader import DBLoadError, LoadRequest, load_table_from_csv
from app.etl.json_to_s3 import ETLError, get_schema_catalog, run_pipeline_all
from app.etl.manifest import ETLManifest, resolve_etl_settings
from app.etl.schema_catalog import SchemaCatalog
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
    context: list[str] = field(default_factory=list)


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
    context: list[str] = field(default_factory=list)


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
        self._intent_classifier = IntentClassifier()
        self._retriever = ChromaRetriever()
        self._max_retries = get_settings().agent_max_retries
        self._repair_knowledge = RepairKnowledge()

    def handle_query(
        self,
        prompt: str,
        *,
        session_id: str | None = None,
    ) -> Tuple[AgentResult, str, List[Dict[str, Any]]]:
        ensure_safe_prompt(prompt)

        active_session = session_id or str(uuid.uuid4())
        history = conversation_history(active_session)
        classified_intent, reuse_last = self._classify_intent(prompt, history, active_session)
        start_time = time.perf_counter()
        log_structured(
            logger,
            logging.INFO,
            "agent_handle_query_start",
            prompt_length=len(prompt),
            history_turns=len(history),
        )

        if reuse_last:
            last_intent_name = get_last_intent(active_session)
            active_intent = Intent[last_intent_name] if last_intent_name in Intent.__members__ else Intent.SQL
        elif classified_intent is not None:
            active_intent = classified_intent
        else:
            last_intent_name = get_last_intent(active_session)
            if last_intent_name in Intent.__members__:
                active_intent = Intent[last_intent_name]
            else:
                active_intent = plan_intent(prompt).intent

        session_token = set_session_id(active_session)
        try:
            append_user_turn(active_session, prompt)
            etl_settings_preview = {}
            if active_intent is Intent.ETL:
                etl_cfg = get_settings().etl
                etl_settings_preview = {
                    "raw_dir": getattr(etl_cfg, "raw_dir", ""),
                    "processed_dir": getattr(etl_cfg, "processed_dir", ""),
                    "manifest": getattr(etl_cfg, "manifest_path", ""),
                }
            log_structured(
                logger,
                logging.INFO,
                "agent_routing",
                classified_intent=getattr(classified_intent, "name", None),
                reuse_last=reuse_last,
                final_intent=active_intent.name,
                etl_settings=etl_settings_preview,
            )
            prompt_history = history + [{"role": "user", "prompt": prompt}]

            if active_intent is Intent.SQL:
                response = self._handle_sql(prompt, history=prompt_history, session_id=active_session)
            elif active_intent is Intent.ETL:
                response = self._handle_etl(prompt, history=prompt_history, session_id=active_session)
            else:
                raise NotImplementedError(f"Intent {active_intent.name} not yet supported")
        finally:
            reset_session_id(session_token)

        self._record_agent_turn(active_session, active_intent, response)
        set_last_intent(active_session, active_intent.name)
        elapsed = time.perf_counter() - start_time
        if isinstance(response, SQLAgentResponse):
            metrics = {
                "row_count": len(response.rows),
                "column_count": len(response.columns),
                "repaired": response.repaired,
            }
        else:
            metrics = {
                "table_count": len(response.results),
                "tables": [
                    {"table": summary.table, "row_count": summary.row_count}
                    for summary in response.results
                ],
                "repaired": response.repaired,
            }
        metrics.update({"attempts": response.attempts, "elapsed_ms": round(elapsed * 1000, 2), "intent": response.intent.name})
        log_structured(logger, logging.INFO, "agent_handle_query_complete", **metrics)

        updated_history = conversation_history(active_session)
        return response, active_session, updated_history

    # ------------------------------------------------------------------
    # SQL path
    # ------------------------------------------------------------------
    def _handle_sql(
        self,
        prompt: str,
        *,
        history: List[Dict[str, Any]],
        session_id: str,
    ) -> SQLAgentResponse:
        settings = get_settings()
        try:
            context = self._retriever.retrieve(prompt)
        except RetrievalError as exc:
            log_structured(
                logger,
                logging.ERROR,
                "sql_retrieval_failed",
                session=session_id,
                error=str(exc),
            )
            raise
        log_structured(
            logger,
            logging.INFO,
            "sql_retrieval",
            context_chunks=len(context),
            history_turns=len(history),
        )

        limit = settings.default_result_limit
        error_messages: list[str] = []
        last_sql: str | None = None
        required_literals = _extract_required_literals(prompt)
        augmented_prompt = _augment_prompt_with_history(prompt, history)

        for attempt in range(1, self._max_retries + 1):
            if attempt == 1 or last_sql is None or not error_messages:
                sql_prompt = build_sql_prompt(augmented_prompt, context, limit=limit)
            else:
                sql_prompt = build_sql_repair_prompt(
                    user_prompt=augmented_prompt,
                    context_chunks=context,
                    previous_sql=last_sql,
                    error_summary=error_messages[-1],
                    limit=limit,
                )

            log_structured(
                logger,
                logging.INFO,
                "sql_generation_attempt",
                attempt=attempt,
                repair=attempt > 1,
            )

            try:
                sql = self._llm.generate(sql_prompt)
                if required_literals:
                    ensure_required_literals(sql, required_literals)
            except (LLMError, GuardrailViolation) as exc:
                summary = summarize_exception(exc)
                error_messages.append(summary.message)
                log_structured(
                    logger,
                    logging.WARNING,
                    "sql_generation_error",
                    attempt=attempt,
                    error=summary.message,
                )
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
                log_structured(
                    logger,
                    logging.INFO,
                    "sql_execution_success",
                    attempt=attempt,
                    row_count=len(result["rows"]),
                    column_count=len(result["columns"]),
                )
                return SQLAgentResponse(
                    sql=result["sql"],
                    rows=result["rows"],
                    columns=result["columns"],
                    intent=Intent.SQL,
                    limit_enforced=result["limit_enforced"],
                    attempts=attempt,
                    repaired=attempt > 1,
                    errors=list(error_messages),
                    context=list(context),
                )
            except (ValidationError, GuardrailViolation, SQLAlchemyError) as exc:
                summary = summarize_exception(exc)
                error_messages.append(summary.message)
                last_sql = sql
                log_structured(
                    logger,
                    logging.WARNING,
                    "sql_execution_error",
                    attempt=attempt,
                    error=summary.message,
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
    def _handle_etl(self, prompt: str, *, history: List[Dict[str, Any]], session_id: str) -> ETLAgentResponse:
        settings = get_settings()
        etl_settings, manifest = resolve_etl_settings(settings.etl)
        catalog = get_schema_catalog(etl_settings)
        augmented_prompt = _augment_prompt_with_history(prompt, history)
        cache_ttl = settings.cache.ttl_seconds if settings.cache else None

        def _as_bool(value: object) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                return value.strip().lower() in {"true", "1", "yes", "on"}
            return False

        source_hints: dict[str, list[str]] | None = None
        auto_mapping_enabled = False
        if manifest:
            auto_mapping_enabled = _as_bool(manifest.transform.get("auto_mapping"))
            raw_hints = manifest.transform.get("source_columns")
            if isinstance(raw_hints, dict):
                source_hints = {
                    str(table): [str(column) for column in value]
                    for table, value in raw_hints.items()
                    if isinstance(value, (list, tuple, set))
                }
        try:
            context = self._retriever.retrieve(prompt)
        except RetrievalError as exc:
            log_structured(
                logger,
                logging.ERROR,
                "etl_retrieval_failed",
                error=str(exc),
            )
            raise
        log_structured(
            logger,
            logging.INFO,
            "etl_retrieval",
            context_chunks=len(context),
            history_turns=len(history),
            manifest=getattr(manifest, "path", None) if manifest else None,
        )

        error_history: list[str] = []
        cache_client = get_client()
        cache_key = None
        error_history_key: str | None = None
        skip_flag_key: str | None = None
        skip_tables: set[str] = set()
        if cache_client:
            cache_key = self._make_etl_cache_key(prompt, etl_settings)
            cached_payload = get_json(cache_key)
            if cached_payload:
                log_structured(logger, logging.INFO, "etl_cache_hit", cache_key=cache_key)
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
                augmented_prompt,
                context,
                error_history=error_history,
            )
            log_structured(
                logger,
                logging.INFO,
                "etl_directive_attempt",
                attempt=attempt,
            )

            try:
                directive_raw = self._llm.generate(etl_prompt)
                directive = self._parse_etl_directive(
                    directive_raw,
                    etl_settings=etl_settings,
                    catalog=catalog,
                )
                table_choice = directive["table"]
                if table_choice != "all":
                    log_structured(
                        logger,
                        logging.INFO,
                        "etl_directive_override",
                        attempt=attempt,
                        table=table_choice,
                    )
                    table_choice = "all"
            except (LLMError, GuardrailViolation) as exc:
                summary = summarize_exception(exc)
                error_history.append(f"Attempt {attempt} directive failed: {summary.message}")
                if error_history_key:
                    set_json(error_history_key, error_history, ttl=cache_ttl)
                log_structured(
                    logger,
                    logging.WARNING,
                    "etl_directive_error",
                    attempt=attempt,
                    error=summary.message,
                )
                if attempt == self._max_retries:
                    raise AgentExecutionError(summary.message, attempts=attempt, errors=list(error_history))
                continue
            except ValueError as exc:
                summary = summarize_exception(exc)
                error_history.append(f"Attempt {attempt} directive invalid: {summary.message}")
                if error_history_key:
                    set_json(error_history_key, error_history, ttl=cache_ttl)
                log_structured(
                    logger,
                    logging.WARNING,
                    "etl_directive_invalid",
                    attempt=attempt,
                    error=summary.message,
                )
                if attempt == self._max_retries:
                    raise AgentExecutionError(summary.message, attempts=attempt, errors=list(error_history))
                continue

            try:
                requested_tables = (
                    _order_tables(list(catalog.table_names))
                    if table_choice == "all"
                    else _order_tables([table_choice])
                )

                column_mappings: dict[str, dict[str, str]] | None = None
                if manifest and auto_mapping_enabled:
                    mapper = SchemaMapper(generate_fn=self._llm.generate)
                    try:
                        column_mappings = mapper.generate_mappings(
                            requested_tables,
                            catalog=catalog,
                            source_hints=source_hints,
                            manifest=manifest,
                            namespace=etl_settings.raw_dir,
                        )
                    except SchemaMappingError as exc:
                        message = f"Schema mapping failed: {exc}"
                        log_structured(logger, logging.WARNING, "schema_mapping_failed", error=message)
                        if message not in error_history:
                            error_history.append(message)
                            if error_history_key:
                                set_json(error_history_key, error_history, ttl=cache_ttl)
                        column_mappings = None

                pipeline_results = run_pipeline_all(
                    etl_settings,
                    manifest=manifest,
                    column_mappings=column_mappings,
                )
                log_structured(
                    logger,
                    logging.INFO,
                    "etl_pipeline_run",
                    attempt=attempt,
                    tables=list(pipeline_results),
                )
                if table_choice == "all":
                    tables = _order_tables(list(pipeline_results))
                else:
                    tables = _order_tables([table_choice])
                db_rows: dict[str, int | None] = {}
                if etl_settings.enable_db_load:
                    load_failure_summary = None
                    prefer_upsert = False
                    if manifest:
                        conflict_strategy = manifest.target.get("on_conflict")
                        if isinstance(conflict_strategy, str) and conflict_strategy.lower() in {"upsert", "do_nothing"}:
                            prefer_upsert = True
                    for table in tables:
                        stored_strategy = self._repair_knowledge.get_strategy(table)
                        load_mode = stored_strategy or ("upsert" if prefer_upsert else "insert")
                        if table in skip_tables and load_mode == "upsert":
                            skip_tables.discard(table)
                            if skip_flag_key:
                                set_json(skip_flag_key, sorted(skip_tables), ttl=cache_ttl)

                        if table in skip_tables:
                            log_structured(
                                logger,
                                logging.INFO,
                                "etl_db_load_skipped",
                                table=table,
                            )
                            info_msg = f"DB load skipped for {table} (duplicate primary key detected earlier)."
                            if info_msg not in error_history:
                                error_history.append(info_msg)
                                if error_history_key:
                                    set_json(error_history_key, error_history, ttl=cache_ttl)
                            db_rows[table] = None
                            continue
                        request = LoadRequest(
                            table=table,
                            csv_path=Path(pipeline_results[table]["local_path"]),
                            truncate_before_load=etl_settings.truncate_before_load,
                            mode=load_mode,
                        )
                        try:
                            result = load_table_from_csv(
                                request,
                                database=settings.database,
                                chunksize=etl_settings.db_chunksize,
                            )
                            db_rows[result.table] = result.inserted_rows
                            if load_mode == "upsert":
                                self._repair_knowledge.record_strategy(table, "upsert", error=None)
                                if skip_flag_key and table in skip_tables:
                                    skip_tables.discard(table)
                                    set_json(skip_flag_key, sorted(skip_tables), ttl=cache_ttl)
                        except DBLoadError as exc:
                            message_lower = str(exc).lower()
                            duplicate_error = (
                                "duplicate key value violates unique constraint" in message_lower
                                or "unique constraint failed" in message_lower
                                or "unique constraint" in message_lower
                            )
                            if duplicate_error and load_mode == "insert":
                                log_structured(
                                    logger,
                                    logging.INFO,
                                    "etl_db_duplicate_key_retry",
                                    table=table,
                                )
                                try:
                                    upsert_result = load_table_from_csv(
                                        LoadRequest(
                                            table=table,
                                            csv_path=Path(pipeline_results[table]["local_path"]),
                                            truncate_before_load=False,
                                            mode="upsert",
                                        ),
                                        database=settings.database,
                                        chunksize=etl_settings.db_chunksize,
                                    )
                                    db_rows[table] = upsert_result.inserted_rows
                                    info_msg = (
                                        f"Duplicate key detected for {table}; retried with UPSERT (ON CONFLICT DO NOTHING)."
                                    )
                                    if info_msg not in error_history:
                                        error_history.append(info_msg)
                                        if error_history_key:
                                            set_json(error_history_key, error_history, ttl=cache_ttl)
                                    self._repair_knowledge.record_strategy(table, "upsert", error=str(exc))
                                    if skip_flag_key and table in skip_tables:
                                        skip_tables.discard(table)
                                        set_json(skip_flag_key, sorted(skip_tables), ttl=cache_ttl)
                                    continue
                                except DBLoadError as upsert_exc:
                                    summary = summarize_exception(upsert_exc)
                                    load_failure_summary = summary
                                    error_history.append(
                                        f"Attempt {attempt} DB load failed after UPSERT retry: {summary.message}"
                                    )
                                    if error_history_key:
                                        set_json(error_history_key, error_history, ttl=cache_ttl)
                                    log_structured(
                                        logger,
                                        logging.WARNING,
                                        "etl_db_load_failed_after_retry",
                                        attempt=attempt,
                                        error=summary.message,
                                    )
                                    self._repair_knowledge.clear_strategy(table)
                                    break
                            if duplicate_error and load_mode == "upsert":
                                self._repair_knowledge.clear_strategy(table)
                            skip_tables.add(table)
                            if skip_flag_key:
                                set_json(skip_flag_key, sorted(skip_tables), ttl=cache_ttl)
                            summary = summarize_exception(exc)
                            load_failure_summary = summary
                            error_history.append(f"Attempt {attempt} DB load failed: {summary.message}")
                            if error_history_key:
                                set_json(error_history_key, error_history, ttl=cache_ttl)
                            log_structured(
                                logger,
                                logging.WARNING,
                                "etl_db_load_failed",
                                attempt=attempt,
                                error=summary.message,
                            )
                            break

                    if load_failure_summary:
                        if attempt == self._max_retries:
                            raise AgentExecutionError(
                                load_failure_summary.message,
                                attempts=attempt,
                                errors=list(error_history),
                            )
                        continue
                else:
                    message = (
                        "Database loading is disabled via configuration; CSVs were generated but not loaded into the database."
                    )
                    if message not in error_history:
                        error_history.append(message)
                    log_structured(logger, logging.INFO, "etl_db_load_disabled", detail=message)

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
                    context=list(context),
                )
                log_structured(
                    logger,
                    logging.INFO,
                    "etl_success",
                    attempt=attempt,
                    tables=[summary.table for summary in summaries],
                )
                if cache_key:
                    set_json(cache_key, _etl_agent_response_to_cache_payload(response), ttl=cache_ttl)
                if error_history_key:
                    cache_delete(error_history_key)
                return response
            except ETLError as exc:
                summary = summarize_exception(exc)
                error_history.append(f"Attempt {attempt} pipeline failed: {summary.message}")
                log_structured(
                    logger,
                    logging.WARNING,
                    "etl_pipeline_failed",
                    attempt=attempt,
                    error=summary.message,
                )
                if attempt == self._max_retries:
                    raise AgentExecutionError(summary.message, attempts=attempt, errors=list(error_history))

        raise AgentExecutionError(
            "Agent failed after ETL retries.",
            attempts=self._max_retries,
            errors=list(error_history),
        )

    @staticmethod
    def _parse_etl_directive(
        payload: str,
        *,
        etl_settings: ETLSettings | None = None,
        catalog: SchemaCatalog | None = None,
    ) -> dict[str, str]:
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
        active_settings = etl_settings or get_settings().etl
        active_catalog = catalog or get_schema_catalog(active_settings)
        if table != "all" and table not in active_catalog.table_names:
            valid = ["all", *sorted(active_catalog.table_names)]
            raise ValueError(
                f"Table '{table}' is not supported. Choose from: {valid}"
            )
        return {"table": table}

    @staticmethod
    def _make_etl_cache_key(prompt: str, etl_settings) -> str:
        prompt_hash = hashlib.md5(prompt.strip().lower().encode("utf-8")).hexdigest()
        raw_dir = Path(etl_settings.raw_dir)
        pattern = getattr(etl_settings, "source_pattern", "*.json") or "*.json"
        entries: list[str] = [f"pattern={pattern}"]
        manifest_path = getattr(etl_settings, "manifest_path", None)
        if manifest_path:
            entries.append(f"manifest={manifest_path}")
        if raw_dir.exists():
            for path in sorted(raw_dir.glob(pattern)):
                try:
                    stat = path.stat()
                except OSError:
                    continue
                entries.append(f"{path.name}:{int(stat.st_mtime)}:{stat.st_size}")
        signature = "|".join(entries)
        dir_hash = hashlib.md5(signature.encode("utf-8")).hexdigest() if signature else "none"
        return f"etl:{prompt_hash}:{dir_hash}"

    def _classify_intent(
        self,
        prompt: str,
        history: List[Dict[str, Any]],
        session_id: str,
    ) -> Tuple[Optional[Intent], bool]:
        try:
            classified, reuse_last = self._intent_classifier.classify(prompt, history)
            return classified, reuse_last
        except Exception as exc:  # pragma: no cover - keep routing resilient
            log_structured(
                logger,
                logging.WARNING,
                "intent_classifier_failed",
                error=str(exc),
            )
            return None, False

    def _record_agent_turn(self, session_id: str, intent: Intent, response: AgentResult) -> None:
        if isinstance(response, SQLAgentResponse):
            rows_preview = response.rows[:5]
            agent_turn = {
                "intent": intent.name,
                "sql": response.sql,
                "row_count": len(response.rows),
                "columns": response.columns,
                "errors": response.errors,
                "rows_preview": rows_preview,
                "summary": f"Returned {len(response.rows)} rows.",
            }
        else:
            results_preview = [
                {"table": summary.table, "row_count": summary.row_count}
                for summary in response.results
            ]
            agent_turn = {
                "intent": intent.name,
                "results": results_preview,
                "errors": response.errors,
                "summary": f"Processed tables: {', '.join(item['table'] for item in results_preview)}",
            }
        append_agent_turn(session_id, agent_turn)


def _build_history_prompt(history: List[Dict[str, Any]], max_turns: int = 4) -> str:
    relevant = history[-max_turns:]
    lines: List[str] = []
    for turn in relevant:
        role = turn.get("role")
        if role == "user":
            prompt_text = turn.get("prompt", "")
            lines.append(f"User: {prompt_text}")
        elif role == "agent":
            intent = turn.get("intent", "agent")
            summary = turn.get("summary") or turn.get("sql") or ""
            if summary and len(summary) > 400:
                summary = summary[:400] + " ..."
            lines.append(f"Agent ({intent}): {summary}")
    return "\n".join(lines).strip()


def _augment_prompt_with_history(prompt: str, history: List[Dict[str, Any]]) -> str:
    history_block = _build_history_prompt(history)
    if not history_block:
        return prompt
    return f"{history_block}\n\nUser: {prompt}"


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
        "context": response.context,
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
    context = payload.get("context") or []
    return ETLAgentResponse(
        results=summaries,
        intent=intent,
        attempts=int(payload.get("attempts", 1)),
        repaired=False,
        errors=list(errors),
        context=list(context) if isinstance(context, list) else [],
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
