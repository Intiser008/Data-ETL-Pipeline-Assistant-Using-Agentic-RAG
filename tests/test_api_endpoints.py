from __future__ import annotations

from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from app.api.main import app, service as service_instance
from app.agent.guardrails import GuardrailViolation
from app.agent.retriever import RetrievalError
from app.agent.service import (
    AgentExecutionError,
    AgentResult,
    ETLAgentResponse,
    ETLTableSummary,
    SQLAgentResponse,
)
from app.agent.planner import Intent


class DummySQLResponse(SQLAgentResponse):
    def __init__(self) -> None:
        super().__init__(
            sql="SELECT 1",
            rows=[{"dummy": 1}],
            columns=["dummy"],
            intent=Intent.SQL,
            limit_enforced=False,
            attempts=1,
            repaired=False,
        )


class DummyETLResponse(ETLAgentResponse):
    def __init__(self) -> None:
        super().__init__(
            results=[
                ETLTableSummary(
                    table="patients",
                    row_count=3,
                    local_path="/tmp/patients.csv",
                    s3_uri=None,
                    loaded_rows=3,
                )
            ],
            intent=Intent.ETL,
            attempts=1,
            repaired=False,
        )


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    # Reset global service state by re-importing AgentService with controlled dependencies.
    class StubService:
        def __init__(self) -> None:
            self._handler = None

        def set_handler(self, handler):
            self._handler = handler

        def handle_query(self, prompt: str, *, session_id: str | None = None):
            if self._handler is None:
                raise RuntimeError("Handler not configured")
            response, sid, history = self._handler(prompt, session_id=session_id)
            return response, sid, history

    stub = StubService()

    def configure(handler):
        stub.set_handler(handler)

    monkeypatch.setattr(service_instance, "handle_query", stub.handle_query)
    monkeypatch.setattr(service_instance, "configure_for_test", configure, raising=False)
    return TestClient(app)


def _success_history() -> list[Dict[str, Any]]:
    return [{"role": "user", "prompt": "hello"}, {"role": "agent", "intent": "SQL"}]


def test_query_sql_success(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(app)

    def fake_handle(prompt: str, *, session_id: str | None = None):
        return DummySQLResponse(), session_id or "session-123", _success_history()

    monkeypatch.setattr(service_instance, "handle_query", fake_handle)

    response = client.post("/query", json={"prompt": "select something"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "SQL"
    assert payload["rows"] == [{"dummy": 1}]
    assert payload["session_id"]


def test_query_guardrail_violation(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(app)

    def fake_handle(prompt: str, *, session_id: str | None = None):
        raise GuardrailViolation("blocked")

    monkeypatch.setattr(service_instance, "handle_query", fake_handle)

    response = client.post("/query", json={"prompt": "DROP TABLE patients"})

    assert response.status_code == 400
    assert response.json()["detail"] == "blocked"


def test_query_retriever_failure(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(app)

    def fake_handle(prompt: str, *, session_id: str | None = None):
        raise RetrievalError("vector store down")

    monkeypatch.setattr(service_instance, "handle_query", fake_handle)

    response = client.post("/query", json={"prompt": "what's wrong?"})

    assert response.status_code == 500
    assert response.json()["detail"] == "Retriever failed"


def test_query_agent_execution_failure(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(app)

    def fake_handle(prompt: str, *, session_id: str | None = None):
        raise AgentExecutionError("failed", attempts=3, errors=["boom"])

    monkeypatch.setattr(service_instance, "handle_query", fake_handle)

    response = client.post("/query", json={"prompt": "run complex thing"})

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["message"] == "failed"
    assert detail["errors"] == ["boom"]


def test_query_etl_success(monkeypatch: pytest.MonkeyPatch):
    client = TestClient(app)

    def fake_handle(prompt: str, *, session_id: str | None = None):
        return DummyETLResponse(), session_id or "session-etl", _success_history()

    monkeypatch.setattr(service_instance, "handle_query", fake_handle)

    response = client.post("/query", json={"prompt": "run full etl"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"] == "ETL"
    assert len(payload["results"]) == 1
    assert payload["results"][0]["table"] == "patients"

