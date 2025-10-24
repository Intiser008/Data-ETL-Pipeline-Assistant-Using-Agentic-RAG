"""FastAPI application exposing the query endpoint."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.agent.guardrails import GuardrailViolation
from app.agent.retriever import RetrievalError
from app.agent.service import AgentService
from app.core.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

app = FastAPI(title="Agentic RAG Data Analytics Assistant")
service = AgentService()


class QueryRequest(BaseModel):
    prompt: str = Field(..., description="Natural language question to answer.")


class QueryResponse(BaseModel):
    sql: str
    intent: str
    limit_enforced: bool
    columns: list[str]
    rows: list[dict]


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

    return QueryResponse(
        sql=response.sql,
        intent=response.intent.name,
        limit_enforced=response.limit_enforced,
        columns=response.columns,
        rows=response.rows,
    )

