"""FastAPI application exposing the query endpoint."""

from __future__ import annotations

from typing import Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.agent.guardrails import GuardrailViolation
from app.agent.retriever import RetrievalError
from app.agent.service import (
    AgentExecutionError,
    AgentService,
    ETLAgentResponse,
    SQLAgentResponse,
)
from app.core.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

app = FastAPI(title="Agentic RAG Data Analytics Assistant")
service = AgentService()


class QueryRequest(BaseModel):
    prompt: str = Field(..., description="Natural language question to answer.")


class SQLQueryResponse(BaseModel):
    sql: str
    intent: str
    limit_enforced: bool
    columns: list[str]
    rows: list[dict]
    attempts: int
    repaired: bool
    errors: list[str]


class ETLTableResponse(BaseModel):
    table: str
    row_count: int
    local_path: str
    s3_uri: str | None
    loaded_rows: int | None


class ETLQueryResponse(BaseModel):
    results: list[ETLTableResponse]
    intent: str
    attempts: int
    repaired: bool
    errors: list[str]


QueryResponse = Union[SQLQueryResponse, ETLQueryResponse]


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest) -> QueryResponse:
    logger.info("Received query request")
    try:
        response = service.handle_query(request.prompt)
    except GuardrailViolation as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RetrievalError as exc:
        raise HTTPException(status_code=500, detail="Retriever failed") from exc
    except NotImplementedError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except AgentExecutionError as exc:
        raise HTTPException(status_code=500, detail={"message": str(exc), "errors": exc.errors}) from exc

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
        )

    raise HTTPException(status_code=500, detail="Unsupported response type")
