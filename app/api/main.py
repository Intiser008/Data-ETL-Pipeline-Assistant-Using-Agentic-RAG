"""FastAPI application exposing the query endpoint."""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, List, Optional, Union
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.agent.guardrails import GuardrailViolation
from app.agent.retriever import RetrievalError
from app.agent.service import (
    AgentExecutionError,
    AgentService,
    ETLAgentResponse,
    SQLAgentResponse,
)
from app.core.logging import (
    configure_logging,
    get_logger,
    log_structured,
    reset_request_id,
    reset_session_id,
    set_request_id,
    set_session_id,
)

configure_logging()
logger = get_logger(__name__)

app = FastAPI(title="Agentic RAG Data Analytics Assistant")
service = AgentService()


@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or str(uuid4())
    request_token = set_request_id(request_id)
    request.state.request_id = request_id

    started = perf_counter()
    try:
        response: Response = await call_next(request)
    except Exception as exc:
        elapsed_ms = round((perf_counter() - started) * 1000, 2)
        log_structured(
            logger,
            logging.ERROR,
            "request_failed",
            path=request.url.path,
            method=request.method,
            elapsed_ms=elapsed_ms,
            error=str(exc),
        )
        raise

    elapsed_ms = round((perf_counter() - started) * 1000, 2)
    log_structured(
        logger,
        logging.INFO,
        "request_completed",
        path=request.url.path,
        method=request.method,
        status=response.status_code,
        elapsed_ms=elapsed_ms,
    )
    reset_request_id(request_token)

    response.headers["x-request-id"] = request_id
    return response


class QueryRequest(BaseModel):
    prompt: str = Field(..., description="Natural language question to answer.")
    session_id: Optional[str] = Field(None, description="Conversation session identifier.")


class SQLQueryResponse(BaseModel):
    sql: str
    intent: str
    limit_enforced: bool
    columns: List[str]
    rows: List[Dict[str, Any]]
    attempts: int
    repaired: bool
    errors: List[str]
    context: List[str]
    session_id: str
    history: List[Dict[str, Any]] = Field(default_factory=list)


class ETLTableResponse(BaseModel):
    table: str
    row_count: int
    local_path: str
    s3_uri: str | None
    loaded_rows: int | None


class ETLQueryResponse(BaseModel):
    results: List[ETLTableResponse]
    intent: str
    attempts: int
    repaired: bool
    errors: List[str]
    context: List[str]
    session_id: str
    history: List[Dict[str, Any]] = Field(default_factory=list)


QueryResponse = Union[SQLQueryResponse, ETLQueryResponse]


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest) -> QueryResponse:
    logger.info("Received query request")
    session_id = request.session_id or str(uuid4())
    session_token = set_session_id(session_id)
    try:
        response, session_id, history = service.handle_query(request.prompt, session_id=session_id)
    except GuardrailViolation as exc:
        log_structured(logger, logging.WARNING, "guardrail_violation", error=str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RetrievalError as exc:
        log_structured(logger, logging.ERROR, "retriever_failure", error=str(exc))
        raise HTTPException(status_code=500, detail="Retriever failed") from exc
    except NotImplementedError as exc:
        log_structured(logger, logging.WARNING, "intent_not_implemented", error=str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentExecutionError as exc:
        log_structured(
            logger,
            logging.ERROR,
            "agent_execution_error",
            attempts=exc.attempts,
            errors=exc.errors,
        )
        raise HTTPException(status_code=500, detail={"message": str(exc), "errors": exc.errors}) from exc
    finally:
        reset_session_id(session_token)

    if isinstance(response, SQLAgentResponse):
        return SQLQueryResponse(
            sql=response.sql,
            intent=response.intent.name,
            limit_enforced=response.limit_enforced,
            columns=response.columns,
            rows=response.rows,
            attempts=response.attempts,
            repaired=response.repaired,
            errors=response.errors,
            context=response.context,
            session_id=session_id,
            history=history,
        )

    if isinstance(response, ETLAgentResponse):
        return ETLQueryResponse(
            results=[
                ETLTableResponse(
                    table=summary.table,
                    row_count=summary.row_count,
                    local_path=summary.local_path,
                    s3_uri=summary.s3_uri,
                    loaded_rows=summary.loaded_rows,
                )
                for summary in response.results
            ],
            intent=response.intent.name,
            attempts=response.attempts,
            repaired=response.repaired,
            errors=response.errors,
            context=response.context,
            session_id=session_id,
            history=history,
        )

    raise HTTPException(status_code=500, detail="Unsupported response type")
