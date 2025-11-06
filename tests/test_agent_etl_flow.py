from __future__ import annotations

from types import SimpleNamespace

from app.core.config import ETLSettings, DatabaseSettings
from app.agent import service as service_module
from app.etl.db_loader import LoadResult


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
        etl=ETLSettings(
            raw_dir=str(tmp_path / "raw"),
            processed_dir=str(tmp_path / "processed"),
            s3_bucket=None,
            s3_prefix="",
            aws_region=None,
            enable_s3=False,
            max_records=0,
            enable_db_load=True,
        ),
        database=DatabaseSettings(url="sqlite://"),
    )

    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)
    (tmp_path / "processed").mkdir(parents=True, exist_ok=True)

    def fake_run_pipeline_all(etl_settings: ETLSettings):
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
    monkeypatch.setattr(
        service_module,
        "load_tables",
        lambda *args, **kwargs: [
            LoadResult(table="patients", inserted_rows=5, source_path=tmp_path / "processed/patients/patients_20240101000000.csv"),
            LoadResult(table="encounters", inserted_rows=3, source_path=tmp_path / "processed/encounters/encounters_20240101000000.csv"),
        ],
    )

    agent = service_module.AgentService()

    response = agent.handle_query("Transform raw patients JSON into a CSV and upload it.")

    assert len(response.results) == 2
    patient_summary = next(item for item in response.results if item.table == "patients")
    encounter_summary = next(item for item in response.results if item.table == "encounters")
    assert patient_summary.row_count == 5
    assert patient_summary.loaded_rows == 5
    assert encounter_summary.loaded_rows == 3
    assert response.intent.name == "ETL"
    assert response.attempts == 1
    assert response.errors == []
