from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from app.core.config import ETLSettings
from app.etl.json_to_s3 import (
    ETLError,
    get_schema_catalog,
    extract,
    load,
    run_pipeline,
    run_pipeline_all,
    transform,
    transform_all,
)


def _make_settings(tmp_path, enable_s3: bool = False) -> ETLSettings:
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    return ETLSettings(
        raw_dir=str(raw_dir),
        processed_dir=str(processed_dir),
        s3_bucket=None,
        s3_prefix="",
        aws_region=None,
        enable_s3=enable_s3,
        max_records=0,
    )


def _write_patient_json(path):
    bundle = _build_full_bundle(include_secondary_resources=False)
    path.write_text(json.dumps(bundle), encoding="utf-8")


def _write_full_sample_json(path):
    bundle = _build_full_bundle(include_secondary_resources=True)
    path.write_text(json.dumps(bundle), encoding="utf-8")


def _build_full_bundle(*, include_secondary_resources: bool) -> dict:
    patient_id = "11111111-1111-1111-1111-111111111111"
    encounter_id = "22222222-2222-2222-2222-222222222222"

    entries = [
        {
            "resource": {
                "resourceType": "Patient",
                "id": patient_id,
                "birthDate": "1980-05-12",
                "deceasedDateTime": None,
                "name": [{"given": ["Alice"], "family": "Smith"}],
                "gender": "female",
                "address": [
                    {
                        "line": ["123 Main St"],
                        "city": "Boston",
                        "state": "MA",
                        "postalCode": "02101",
                        "country": "USA",
                    }
                ],
                "extension": [
                    {"url": "http://example.com/ext/location", "valueAddress": {"city": "Boston"}},
                    {
                        "url": "http://hl7.org/fhir/StructureDefinition/patient-socialSecurityNumber",
                        "valueString": "111-22-3333",
                    },
                ],
            }
        }
    ]

    if not include_secondary_resources:
        return {"entry": entries}

    entries.extend(
        [
            {
                "resource": {
                    "resourceType": "Encounter",
                    "id": encounter_id,
                    "period": {"start": "2024-01-02"},
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "type": [{"coding": [{"code": "E1"}], "text": "Annual Checkup"}],
                    "reasonCode": [{"coding": [{"code": "R1"}], "text": "General wellness"}],
                }
            },
            {
                "resource": {
                    "resourceType": "Condition",
                    "onsetDateTime": "2024-01-03",
                    "abatementDateTime": None,
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{encounter_id}"},
                    "code": {"coding": [{"code": "C1"}], "text": "Hypertension"},
                }
            },
            {
                "resource": {
                    "resourceType": "Observation",
                    "effectiveDateTime": "2024-01-04",
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{encounter_id}"},
                    "code": {"coding": [{"code": "O1"}], "text": "Systolic blood pressure"},
                    "valueQuantity": {"value": 120, "unit": "mmHg"},
                }
            },
            {
                "resource": {
                    "resourceType": "MedicationRequest",
                    "authoredOn": "2024-01-05",
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{encounter_id}"},
                    "medicationCodeableConcept": {
                        "coding": [{"code": "M1"}],
                        "text": "Lisinopril 10mg",
                    },
                    "reasonCode": [{"coding": [{"code": "R1"}], "text": "Hypertension"}],
                }
            },
            {
                "resource": {
                    "resourceType": "Procedure",
                    "performedDateTime": "2024-01-06",
                    "subject": {"reference": f"Patient/{patient_id}"},
                    "encounter": {"reference": f"Encounter/{encounter_id}"},
                    "code": {"coding": [{"code": "P1"}], "text": "EKG"},
                    "reasonCode": [{"coding": [{"code": "R1"}], "text": "Hypertension"}],
                }
            },
        ]
    )

    return {"entry": entries}


def test_extract_discovers_json_files(tmp_path):
    settings = _make_settings(tmp_path)
    file_a = tmp_path / "raw" / "bundle-a.json"
    file_b = tmp_path / "raw" / "bundle-b.json"
    file_a.write_text("{}", encoding="utf-8")
    file_b.write_text("{}", encoding="utf-8")

    paths = extract(settings)

    assert [path.name for path in paths] == ["bundle-a.json", "bundle-b.json"]


def test_extract_raises_when_no_files(tmp_path):
    settings = _make_settings(tmp_path)
    (tmp_path / "raw").mkdir(parents=True, exist_ok=True)

    with pytest.raises(ETLError):
        extract(settings)


def test_transform_patients_returns_expected_dataframe(tmp_path):
    sample_path = tmp_path / "raw" / "patients.json"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    _write_patient_json(sample_path)
    settings = _make_settings(tmp_path)
    catalog = get_schema_catalog(settings)

    df = transform([sample_path], "patients", catalog=catalog)

    assert df.shape[0] == 1
    assert set(df.columns) == set(catalog.get_columns("patients"))
    row = df.iloc[0]
    assert row["id"] == "11111111-1111-1111-1111-111111111111"
    assert row["first"] == "Alice"
    assert "123 Main St" in row["address"]


def test_load_writes_csv_without_s3(tmp_path):
    settings = _make_settings(tmp_path, enable_s3=False)
    catalog = get_schema_catalog(settings)
    data = {
        column: [None]
        for column in catalog.get_columns("patients")
    }
    data["id"] = ["11111111-1111-1111-1111-111111111111"]
    df = pd.DataFrame(data)

    result = load(df, "patients", settings)

    local_path = result["local_path"]
    assert result["s3_uri"] is None
    assert Path(local_path).exists()


def test_run_pipeline_end_to_end(tmp_path):
    settings = _make_settings(tmp_path, enable_s3=False)
    sample_path = tmp_path / "raw" / "patients.json"
    _write_patient_json(sample_path)

    result = run_pipeline("patients", settings)

    assert result["row_count"] == 1
    assert result["s3_uri"] is None
    assert Path(result["local_path"]).exists()


def test_transform_all_creates_all_tables(tmp_path):
    sample_path = tmp_path / "raw" / "full.json"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    _write_full_sample_json(sample_path)
    settings = _make_settings(tmp_path)
    catalog = get_schema_catalog(settings)

    datasets = transform_all([sample_path], catalog=catalog)

    assert set(datasets) == set(catalog.table_names)
    for table, df in datasets.items():
        assert not df.empty, table
        assert set(df.columns) == set(catalog.get_columns(table))


def test_run_pipeline_all_writes_every_table(tmp_path):
    settings = _make_settings(tmp_path, enable_s3=False)
    sample_path = tmp_path / "raw" / "full.json"
    _write_full_sample_json(sample_path)
    catalog = get_schema_catalog(settings)

    results = run_pipeline_all(settings)

    assert set(results) == set(catalog.table_names)
    for table, metadata in results.items():
        assert metadata["row_count"] >= 1
        assert Path(metadata["local_path"]).exists()


def test_custom_schema_config_is_honoured(tmp_path):
    settings = _make_settings(tmp_path, enable_s3=False)
    schema_path = tmp_path / "schema.json"
    schema_payload = {
        "tables": {
            "patients": {
                "columns": [
                    "id",
                    "birthdate",
                    "first",
                    "last"
                ],
                "resource_types": ["Patient"]
            }
        }
    }
    schema_path.write_text(json.dumps(schema_payload), encoding="utf-8")
    settings = replace(settings, schema_config_path=str(schema_path))
    catalog = get_schema_catalog(settings)
    sample_path = tmp_path / "raw" / "patients.json"
    _write_patient_json(sample_path)

    df = transform([sample_path], "patients", catalog=catalog)

    assert set(df.columns) == {"id", "birthdate", "first", "last"}
