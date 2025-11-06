"""Schema catalog helpers for ETL pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Mapping

from app.core.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class TableDefinition:
    """Represents a tabular dataset that can be produced by the ETL pipeline."""

    name: str
    columns: List[str]
    resource_types: List[str]

    @staticmethod
    def from_mapping(name: str, payload: Mapping[str, object]) -> "TableDefinition":
        try:
            columns_raw = payload["columns"]
        except KeyError as exc:  # pragma: no cover - configuration error surfaced to user
            raise ValueError(f"Table '{name}' missing required 'columns' field.") from exc

        resource_types_raw = payload.get("resource_types") or []
        if not isinstance(columns_raw, Iterable):
            raise ValueError(f"Table '{name}' columns must be a list.")
        columns = [str(column).strip() for column in columns_raw if str(column).strip()]
        resource_types = [str(item).strip() for item in resource_types_raw if str(item).strip()]
        return TableDefinition(name=name, columns=columns, resource_types=resource_types)


@dataclass(frozen=True)
class SchemaCatalog:
    """Holds table definitions and lookup helpers for FHIR resource types."""

    tables: Dict[str, TableDefinition]

    def __post_init__(self) -> None:
        if not self.tables:
            raise ValueError("Schema catalog must include at least one table.")

    @property
    def table_names(self) -> List[str]:
        return sorted(self.tables)

    def get_columns(self, table: str) -> List[str]:
        return self.tables[table].columns

    def table_for_resource(self, resource_type: str) -> str | None:
        normalized = resource_type.strip()
        for table in self.tables.values():
            if normalized in table.resource_types:
                return table.name
        return None

    def ensure_table(self, table: str) -> TableDefinition:
        try:
            return self.tables[table]
        except KeyError as exc:
            raise ValueError(f"Unsupported table '{table}'. Available: {self.table_names}") from exc

    @staticmethod
    def default() -> "SchemaCatalog":
        """Return catalog representing the baked-in healthcare schema."""

        definitions = {
            "patients": TableDefinition(
                name="patients",
                columns=[
                    "id",
                    "birthdate",
                    "deathdate",
                    "ssn",
                    "drivers",
                    "passport",
                    "prefix",
                    "first",
                    "last",
                    "suffix",
                    "maiden",
                    "marital",
                    "race",
                    "ethnicity",
                    "gender",
                    "birthplace",
                    "address",
                ],
                resource_types=["Patient"],
            ),
            "encounters": TableDefinition(
                name="encounters",
                columns=[
                    "id",
                    "date",
                    "patient",
                    "code",
                    "description",
                    "reasoncode",
                    "reasondescription",
                ],
                resource_types=["Encounter"],
            ),
            "conditions": TableDefinition(
                name="conditions",
                columns=[
                    "start",
                    "stop",
                    "patient",
                    "encounter",
                    "code",
                    "description",
                ],
                resource_types=["Condition"],
            ),
            "observations": TableDefinition(
                name="observations",
                columns=[
                    "date",
                    "patient",
                    "encounter",
                    "code",
                    "description",
                    "value",
                    "units",
                ],
                resource_types=["Observation"],
            ),
            "medications": TableDefinition(
                name="medications",
                columns=[
                    "start",
                    "stop",
                    "patient",
                    "encounter",
                    "code",
                    "description",
                    "reasoncode",
                    "reasondescription",
                ],
                resource_types=["MedicationRequest", "MedicationOrder", "MedicationPrescription"],
            ),
            "procedures": TableDefinition(
                name="procedures",
                columns=[
                    "date",
                    "patient",
                    "encounter",
                    "code",
                    "description",
                    "reasoncode",
                    "reasondescription",
                ],
                resource_types=["Procedure"],
            ),
        }
        return SchemaCatalog(tables=definitions)

    @staticmethod
    def from_file(path: str | Path) -> "SchemaCatalog":
        path_obj = Path(path)
        if not path_obj.exists():
            raise FileNotFoundError(f"Schema configuration file not found: {path_obj}")

        payload = json.loads(path_obj.read_text(encoding="utf-8"))
        tables_raw = payload.get("tables")
        if not isinstance(tables_raw, Mapping):
            raise ValueError("Schema configuration file must include a 'tables' object.")

        definitions: Dict[str, TableDefinition] = {}
        for table_name, table_payload in tables_raw.items():
            definitions[table_name] = TableDefinition.from_mapping(table_name, table_payload)

        logger.info("Loaded schema catalog from %s", path_obj)
        return SchemaCatalog(tables=definitions)


@lru_cache(maxsize=4)
def load_catalog(path: str | None) -> SchemaCatalog:
    """Cached helper to build catalog from optional path."""
    if path:
        return SchemaCatalog.from_file(path)
    return SchemaCatalog.default()

