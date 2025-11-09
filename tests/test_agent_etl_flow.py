from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from app.core.config import ETLSettings, DatabaseSettings
from app.agent import service as service_module
from app.etl.db_loader import DBLoadError, LoadResult


def test_agent_service_handles_etl_intent(monkeypatch, tmp_path):
    class StubRetriever:
        def retrieve(self, prompt: str):
            return ["etl documentation chunk"]

    class StubLLM:
        def generate(self, prompt: str) -> str:
            return '{"table": "patients"}'

    settings = SimpleNamespace(
        agent_max_retries=2,
        default_result_limit=100,
        llm_timeout_seconds=5,
        etl=ETLSettings(
            raw_dir=str(tmp_path / "raw"),
            processed_dir=str(tmp_path / "processed"),
            s3_bucket=None,
            s3_prefix="",
            aws_region=None,
            enable_s3=False,
            max_records=0,
            enable_db_load=True,
            manifest_path="",
        ),
        database=DatabaseSettings(url="sqlite://"),
        cache=None,
    )

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "processed").mkdir(parents=True, exist_ok=True)

    def fake_run_pipeline_all(etl_settings: ETLSettings, *, manifest=None, column_mappings=None):
        assert etl_settings is settings.etl
        results: dict[str, dict[str, str | int | None]] = {}
        for table in ("patients", "encounters"):
            processed_table_dir = tmp_path / "processed" / table
            processed_table_dir.mkdir(parents=True, exist_ok=True)
            local_path = processed_table_dir / f"{table}_20240101000000.csv"
            local_path.write_text("id\nstub\n", encoding="utf-8")
            results[table] = {
                "table": table,
                "row_count": 5,
                "local_path": str(local_path),
                "s3_uri": None,
            }
        return results

    monkeypatch.setattr(service_module, "ChromaRetriever", lambda: StubRetriever())
    monkeypatch.setattr(service_module, "LambdaLLMClient", lambda: StubLLM())
    monkeypatch.setattr(service_module, "run_pipeline_all", fake_run_pipeline_all)
    monkeypatch.setattr(service_module, "get_settings", lambda: settings)
    monkeypatch.setattr(service_module, "get_client", lambda: None)
    def fake_load_table_from_csv(request, *, database, chunksize):
        inserted = 5 if request.table == "patients" else 3
        source_path = tmp_path / f"processed/{request.table}/{request.table}_20240101000000.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        if not source_path.exists():
            source_path.write_text("id\nstub\n", encoding="utf-8")
        return LoadResult(table=request.table, inserted_rows=inserted, source_path=source_path)

    monkeypatch.setattr(service_module, "load_table_from_csv", fake_load_table_from_csv)

    agent = service_module.AgentService()

    response, session_id, history = agent.handle_query("Transform raw patients JSON into a CSV and upload it.")

    assert len(response.results) == 2
    patient_summary = next(item for item in response.results if item.table == "patients")
    encounter_summary = next(item for item in response.results if item.table == "encounters")
    assert patient_summary.row_count == 5
    assert patient_summary.loaded_rows == 5
    assert encounter_summary.loaded_rows == 3
    assert response.intent.name == "ETL"
    assert response.attempts == 1
    assert response.errors == []
    assert isinstance(session_id, str) and session_id
    assert isinstance(history, list)


def test_agent_service_retries_with_upsert_on_duplicate(monkeypatch, tmp_path):
    class StubRetriever:
        def retrieve(self, prompt: str):
            return ["etl documentation chunk"]

    class StubLLM:
        def generate(self, prompt: str) -> str:
            return '{"table": "patients"}'

    knowledge_instances = []

    class StubRepairKnowledge:
        def __init__(self):
            self.strategies: dict[str, str] = {}
            knowledge_instances.append(self)

        def get_strategy(self, table: str) -> str | None:
            return self.strategies.get(table)

        def record_strategy(self, table: str, strategy: str, error: str | None = None) -> None:
            self.strategies[table] = strategy

        def clear_strategy(self, table: str) -> None:
            self.strategies.pop(table, None)

    settings = SimpleNamespace(
        agent_max_retries=1,
        default_result_limit=100,
        llm_timeout_seconds=5,
        etl=ETLSettings(
            raw_dir=str(tmp_path / "raw"),
            processed_dir=str(tmp_path / "processed"),
            s3_bucket=None,
            s3_prefix="",
            aws_region=None,
            enable_s3=False,
            max_records=0,
            enable_db_load=True,
            manifest_path="",
        ),
        database=DatabaseSettings(url="sqlite://"),
        cache=None,
    )

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "processed" / "patients").mkdir(parents=True, exist_ok=True)
    processed_path = tmp_path / "processed" / "patients" / "patients_20240101000000.csv"
    processed_path.write_text("id\nstub\n", encoding="utf-8")

    def fake_run_pipeline_all(etl_settings: ETLSettings, *, manifest=None, column_mappings=None):
        return {
            "patients": {
                "table": "patients",
                "row_count": 1,
                "local_path": str(processed_path),
                "s3_uri": None,
            }
        }

    calls: list[str] = []

    def fake_load_table_from_csv(request, *, database, chunksize):
        calls.append(request.mode)
        if request.mode == "insert":
            raise DBLoadError("duplicate key value violates unique constraint")
        return LoadResult(table=request.table, inserted_rows=1, source_path=Path(request.csv_path))

    monkeypatch.setattr(service_module, "ChromaRetriever", lambda: StubRetriever())
    monkeypatch.setattr(service_module, "LambdaLLMClient", lambda: StubLLM())
    monkeypatch.setattr(service_module, "RepairKnowledge", lambda: StubRepairKnowledge())
    monkeypatch.setattr(service_module, "run_pipeline_all", fake_run_pipeline_all)
    monkeypatch.setattr("app.etl.json_to_s3.run_pipeline_all", fake_run_pipeline_all)
    monkeypatch.setattr(service_module, "load_table_from_csv", fake_load_table_from_csv)
    monkeypatch.setattr("app.etl.db_loader.load_table_from_csv", fake_load_table_from_csv)
    monkeypatch.setattr(service_module, "get_settings", lambda: settings)
    monkeypatch.setattr(service_module, "get_client", lambda: None)

    from app.etl import json_to_s3 as json_module
    from app.etl import db_loader as db_loader_module

    assert service_module.run_pipeline_all is fake_run_pipeline_all
    assert json_module.run_pipeline_all is fake_run_pipeline_all
    assert service_module.load_table_from_csv is fake_load_table_from_csv
    assert db_loader_module.load_table_from_csv is fake_load_table_from_csv
    assert service_module.get_settings() is settings

    agent = service_module.AgentService()

    response, session_id, history = agent.handle_query("Transform raw patients JSON into a CSV and upload it.")

    assert response.results[0].loaded_rows == 1
    assert ["insert", "upsert"] == calls
    assert knowledge_instances and knowledge_instances[0].strategies.get("patients") == "upsert"
    assert isinstance(session_id, str) and session_id
    assert isinstance(history, list)
