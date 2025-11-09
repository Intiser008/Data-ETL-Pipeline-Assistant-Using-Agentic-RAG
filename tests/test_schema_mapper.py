from __future__ import annotations

import json

import pytest

from app.agent.schema_mapper import SchemaMapper, SchemaMappingCache, SchemaMappingError
from app.etl.schema_catalog import SchemaCatalog, TableDefinition


def _make_catalog() -> SchemaCatalog:
    patients = TableDefinition(
        name="patients",
        columns=["id", "first", "last"],
        resource_types=[],
    )
    return SchemaCatalog(tables={"patients": patients})


def test_schema_mapper_generates_and_caches(tmp_path):
    prompts: list[str] = []

    def fake_generate(prompt: str) -> str:
        prompts.append(prompt)
        return json.dumps({"columns": {"id": "PatientID", "first": "FirstName", "last": "LastName"}})

    cache = SchemaMappingCache(tmp_path / "cache.json")
    mapper = SchemaMapper(generate_fn=fake_generate, cache=cache)
    catalog = _make_catalog()

    mappings = mapper.generate_mappings(
        ["patients"],
        catalog=catalog,
        source_hints={"patients": ["PatientID", "FirstName", "LastName"]},
        namespace="demo",
    )

    assert mappings["patients"]["id"] == "PatientID"
    assert len(prompts) == 1

    # Second call should hit the cache and avoid prompting
    mapper.generate_mappings(
        ["patients"],
        catalog=catalog,
        source_hints={"patients": ["PatientID", "FirstName", "LastName"]},
        namespace="demo",
    )
    assert len(prompts) == 1


def test_schema_mapper_raises_on_invalid_json(tmp_path):
    def bad_generate(prompt: str) -> str:
        return "not-json"

    mapper = SchemaMapper(generate_fn=bad_generate, cache=SchemaMappingCache(tmp_path / "cache.json"))
    catalog = _make_catalog()

    with pytest.raises(SchemaMappingError):
        mapper.generate_mappings(
            ["patients"],
            catalog=catalog,
            source_hints={"patients": ["PatientID"]},
            namespace="demo",
        )

