from __future__ import annotations

from typing import Dict, List

import pytest
from fastapi.testclient import TestClient

from app.api import main as api_main
from app.agent import service as service_module
from app.agent.planner import Intent


class StubLLM:
    def __init__(self) -> None:
        self.requests: List[str] = []

    def generate(self, prompt: str) -> str:
        self.requests.append(prompt)
        if "Return ONLY the JSON object" in prompt:
            return '{"table": "patients"}'
        return "SELECT id, first FROM patients LIMIT 5"


class StubRetriever:
    def retrieve(self, prompt: str) -> List[str]:
        return ["patients table schema", "encounters relationship"]


class StubIntentClassifier:
    def classify(self, prompt: str, history: List[Dict[str, str]]):
        lowered = prompt.lower()
        if "etl" in lowered or "transform" in lowered:
            return Intent.ETL, False
        return Intent.SQL, False


def _memory_conversation_store(monkeypatch: pytest.MonkeyPatch) -> None:
    history: Dict[str, List[dict]] = {}
    intents: Dict[str, str] = {}

    def append_user(session_id: str, prompt: str) -> None:
        history.setdefault(session_id, []).append({"role": "user", "prompt": prompt})

    def append_agent(session_id: str, turn: dict) -> None:
        history.setdefault(session_id, []).append({**turn, "role": "agent"})

    def get_history(session_id: str) -> List[dict]:
        return list(history.get(session_id, []))

    def get_last_intent(session_id: str) -> str | None:
        return intents.get(session_id)

    def set_last_intent(session_id: str, intent: str) -> None:
        intents[session_id] = intent

    monkeypatch.setattr(service_module, "append_user_turn", append_user)
    monkeypatch.setattr(service_module, "append_agent_turn", append_agent)
    monkeypatch.setattr(service_module, "conversation_history", get_history)
    monkeypatch.setattr(service_module, "get_last_intent", get_last_intent)
    monkeypatch.setattr(service_module, "set_last_intent", set_last_intent)


@pytest.fixture
def smoke_client(monkeypatch: pytest.MonkeyPatch, tmp_path) -> TestClient:
    stub_llm = StubLLM()

    etl_settings = type(
        "ETL",
        (),
        {
            "raw_dir": str(tmp_path / "raw"),
            "processed_dir": str(tmp_path / "processed"),
            "manifest_path": "",
            "schema_config_path": None,
            "source_pattern": "*.json",
            "enable_db_load": False,
        },
    )()
    database_settings = type("DB", (), {"url": "sqlite://"})()
    cache_settings = type("Cache", (), {"ttl_seconds": 60})()
    stub_settings = type(
        "Settings",
        (),
        {
            "agent_max_retries": 2,
            "default_result_limit": 5,
            "llm_timeout_seconds": 5,
            "etl": etl_settings,
            "database": database_settings,
            "cache": cache_settings,
        },
    )()

    monkeypatch.setattr(service_module, "get_settings", lambda: stub_settings)
    monkeypatch.setattr(service_module, "get_client", lambda: None)
    monkeypatch.setattr(service_module, "LambdaLLMClient", lambda: stub_llm)
    monkeypatch.setattr(service_module, "ChromaRetriever", lambda: StubRetriever())
    monkeypatch.setattr(service_module, "IntentClassifier", lambda: StubIntentClassifier())

    def fake_execute_query(sql: str, params=None):
        return {
            "sql": sql,
            "rows": [{"id": "1", "first": "Alice"}],
            "columns": ["id", "first"],
            "limit_enforced": False,
        }

    monkeypatch.setattr(service_module, "execute_query", fake_execute_query)

    def fake_run_pipeline_all(etl_settings, *, manifest=None, column_mappings=None):
        return {
            "patients": {
                "table": "patients",
                "row_count": 3,
                "local_path": str(tmp_path / "patients.csv"),
                "s3_uri": None,
            }
        }

    monkeypatch.setattr(service_module, "run_pipeline_all", fake_run_pipeline_all)
    _memory_conversation_store(monkeypatch)

    return TestClient(api_main.app)


def test_smoke_sql_then_etl(smoke_client: TestClient):
    sql_response = smoke_client.post("/query", json={"prompt": "List patient first names."})
    assert sql_response.status_code == 200
    payload = sql_response.json()
    assert payload["intent"] == "SQL"
    assert payload["rows"][0]["first"] == "Alice"
    session_id = payload["session_id"]
    assert payload["history"]

    etl_response = smoke_client.post(
        "/query", json={"prompt": "Run full ETL for latest patients.", "session_id": session_id}
    )
    assert etl_response.status_code == 200
    etl_payload = etl_response.json()
    assert etl_payload["intent"] == "ETL"
    assert etl_payload["results"][0]["row_count"] == 3
    assert etl_payload["session_id"] == session_id
    assert len(etl_payload["history"]) >= 3

