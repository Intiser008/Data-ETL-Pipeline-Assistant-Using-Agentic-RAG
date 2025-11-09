from __future__ import annotations

from types import SimpleNamespace
from typing import Iterator, List

import pytest
from sqlalchemy.exc import SQLAlchemyError

from app.agent import service as service_module
from app.agent.planner import Intent


class StubRetriever:
    def __init__(self, documents: List[str] | None = None) -> None:
        self._documents = documents or ["schema doc"]

    def retrieve(self, prompt: str) -> List[str]:
        return list(self._documents)


class StubIntentClassifier:
    def classify(self, prompt: str, history: List[dict[str, str]]):
        return Intent.SQL, False


def _install_memory_conversation_store(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, list[dict]] = {}
    intents: dict[str, str] = {}

    def append_user(session_id: str, prompt: str) -> None:
        store.setdefault(session_id, []).append({"role": "user", "prompt": prompt})

    def append_agent(session_id: str, turn: dict) -> None:
        store.setdefault(session_id, []).append({**turn, "role": "agent"})

    def history(session_id: str) -> list[dict]:
        return list(store.get(session_id, []))

    def set_intent(session_id: str, intent: str) -> None:
        intents[session_id] = intent

    def get_intent(session_id: str) -> str | None:
        return intents.get(session_id)

    monkeypatch.setattr(service_module, "append_user_turn", append_user)
    monkeypatch.setattr(service_module, "append_agent_turn", append_agent)
    monkeypatch.setattr(service_module, "conversation_history", history)
    monkeypatch.setattr(service_module, "set_last_intent", set_intent)
    monkeypatch.setattr(service_module, "get_last_intent", get_intent)


def _build_settings(tmp_path) -> SimpleNamespace:
    etl_settings = SimpleNamespace(
        raw_dir=str(tmp_path / "raw"),
        processed_dir=str(tmp_path / "processed"),
        manifest_path="",
    )
    database_settings = SimpleNamespace(url="sqlite://")
    cache_settings = SimpleNamespace(ttl_seconds=60)

    return SimpleNamespace(
        agent_max_retries=2,
        default_result_limit=100,
        llm_timeout_seconds=5,
        etl=etl_settings,
        database=database_settings,
        cache=cache_settings,
    )


def _install_common_patches(monkeypatch: pytest.MonkeyPatch, tmp_path, llm_instance) -> None:
    settings = _build_settings(tmp_path)
    monkeypatch.setattr(service_module, "ChromaRetriever", lambda: StubRetriever())
    monkeypatch.setattr(service_module, "LambdaLLMClient", lambda: llm_instance)
    monkeypatch.setattr(service_module, "IntentClassifier", lambda: StubIntentClassifier())
    monkeypatch.setattr(service_module, "get_settings", lambda: settings)
    monkeypatch.setattr(service_module, "get_client", lambda: None)
    _install_memory_conversation_store(monkeypatch)


def test_prompt_level_guardrail_blocks_ddl(monkeypatch, tmp_path):
    class NoOpLLM:
        def generate(self, prompt: str) -> str:
            return "SELECT 1"

    _install_common_patches(monkeypatch, tmp_path, NoOpLLM())

    agent = service_module.AgentService()

    with pytest.raises(service_module.GuardrailViolation):
        agent.handle_query("DROP TABLE patients;")


def test_unknown_column_triggers_guardrail(monkeypatch, tmp_path):
    class FixedLLM:
        def generate(self, prompt: str) -> str:
            return "SELECT imaginary_column FROM patients LIMIT 1"

    _install_common_patches(monkeypatch, tmp_path, FixedLLM())
    settings = service_module.get_settings()
    settings.agent_max_retries = 1

    agent = service_module.AgentService()

    with pytest.raises(service_module.AgentExecutionError) as exc:
        agent.handle_query("Show me the imaginary column for patients.")

    assert any("Unknown column" in message for message in exc.value.errors)


def test_literal_enforcement_blocks_missing_literals(monkeypatch, tmp_path):
    class LiteralOmittingLLM:
        def generate(self, prompt: str) -> str:
            return "SELECT id FROM patients LIMIT 10"

    _install_common_patches(monkeypatch, tmp_path, LiteralOmittingLLM())
    settings = service_module.get_settings()
    settings.agent_max_retries = 1

    agent = service_module.AgentService()

    with pytest.raises(service_module.AgentExecutionError) as exc:
        agent.handle_query("List patients born on 2024-01-01.")

    assert any("omitted required values" in message.lower() for message in exc.value.errors)


def test_retry_and_repair_succeeds(monkeypatch, tmp_path):
    responses: Iterator[str] = iter(
        [
            "SELECT id FROM patients LIMIT 5",
            "SELECT id, first FROM patients LIMIT 5",
        ]
    )

    class SequenceLLM:
        def generate(self, prompt: str) -> str:
            try:
                return next(responses)
            except StopIteration:
                return "SELECT id FROM patients LIMIT 5"

    calls: list[str] = []

    def fake_execute_query(sql: str, params=None):
        calls.append(sql)
        if len(calls) == 1:
            raise SQLAlchemyError("syntax error at or near FROM")
        return {
            "sql": sql,
            "rows": [{"id": "1", "first": "Alice"}],
            "columns": ["id", "first"],
            "limit_enforced": False,
        }

    _install_common_patches(monkeypatch, tmp_path, SequenceLLM())
    monkeypatch.setattr(service_module, "execute_query", fake_execute_query)
    settings = service_module.get_settings()
    settings.agent_max_retries = 2

    agent = service_module.AgentService()

    response, session_id, history = agent.handle_query("Show first names for patients.")

    assert response.repaired is True
    assert response.attempts == 2
    assert response.rows[0]["first"] == "Alice"
    assert len(history) >= 1

