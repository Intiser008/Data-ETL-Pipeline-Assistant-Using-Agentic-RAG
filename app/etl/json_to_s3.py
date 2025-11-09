from __future__ import annotations

import argparse
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence

import pandas as pd

from app.core.config import ETLSettings, get_settings
from app.core.logging import get_logger
from app.etl.connectors import LocalFileConnector, S3Connector, StorageError
from app.etl.manifest import ETLManifest, resolve_etl_settings
from app.etl.schema_catalog import SchemaCatalog, load_catalog
from app.etl.schema_utils import normalize_date_columns

logger = get_logger(__name__)


class ETLError(RuntimeError):
    """Raised when the JSON -> CSV -> S3 ETL pipeline fails."""


def get_schema_catalog(settings: ETLSettings | None = None) -> SchemaCatalog:
    """Return the schema catalog, optionally using overrides from settings."""
    path = settings.schema_config_path if settings else get_settings().etl.schema_config_path
    return load_catalog(path)


def _build_resource_map(catalog: SchemaCatalog) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for table in catalog.tables.values():
        for resource in table.resource_types:
            mapping[resource] = table.name
    return mapping


def extract(settings: ETLSettings) -> List[Path]:
    """Collect all JSON bundles under the configured raw directory."""

    raw_dir = Path(settings.raw_dir)
    if not raw_dir.exists():
        raise ETLError(f"Raw directory not found: {raw_dir}")

    pattern = settings.source_pattern or "*.json"
    file_paths = sorted(path for path in raw_dir.glob(pattern) if path.is_file())
    if not file_paths:
        raise ETLError(f"No JSON bundles found under {raw_dir} matching pattern '{pattern}'")

    logger.info("Discovered %s raw JSON files", len(file_paths))
    return file_paths


def transform(
    file_paths: Iterable[Path],
    table: str,
    *,
    max_records: int = 0,
    catalog: SchemaCatalog | None = None,
    column_mapping: Mapping[str, str] | None = None,
) -> pd.DataFrame:
    """Convert the JSON bundles into a dataframe matching the requested table schema."""

    catalog = catalog or get_schema_catalog()
    definition = catalog.ensure_table(table.lower())

    datasets = transform_all(
        file_paths,
        max_records=max_records,
        catalog=catalog,
        require_all_tables=False,
    )
    df = datasets.get(definition.name)
    if df is None or df.empty:
        raise ETLError(f"No records produced for table '{definition.name}'.")

    logger.info("Transform produced %s rows for table '%s'", len(df), definition.name)
    if column_mapping:
        df = _apply_column_mapping(df, column_mapping, catalog.get_columns(definition.name))
    return df


def transform_all(
    file_paths: Iterable[Path],
    *,
    max_records: int = 0,
    catalog: SchemaCatalog | None = None,
    require_all_tables: bool = True,
    column_mappings: Mapping[str, Mapping[str, str]] | None = None,
) -> Dict[str, pd.DataFrame]:
    """Transform JSON bundles into dataframes for every supported table."""

    catalog = catalog or get_schema_catalog()
    buffers: Dict[str, List[dict]] = {name: [] for name in catalog.table_names}
    resource_map = _build_resource_map(catalog)

    for path in file_paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ETLError(f"Failed to decode JSON file {path.name}: {exc}") from exc

        for entry in data.get("entry", []):
            resource = entry.get("resource", {})
            resource_type = resource.get("resourceType")
            if not resource_type:
                continue
            target_table = resource_map.get(resource_type)
            if not target_table:
                continue

            record = _resource_to_record(resource, target_table)
            if record is not None:
                buffers[target_table].append(record)

    datasets: Dict[str, pd.DataFrame] = {}
    missing: list[str] = []

    for table_name in catalog.table_names:
        records = buffers.get(table_name) or []
        if not records:
            missing.append(table_name)
            continue

        df = pd.DataFrame(records, columns=catalog.get_columns(table_name))
        df.drop_duplicates(inplace=True)
        df = normalize_date_columns(df, table_name)
        if max_records and len(df) > max_records:
            df = df.head(max_records).copy()
        if column_mappings:
            mapping = column_mappings.get(table_name)
            if mapping:
                df = _apply_column_mapping(df, mapping, catalog.get_columns(table_name))
        datasets[table_name] = df

    if missing and require_all_tables:
        raise ETLError(
            "No records produced for tables: "
            + ", ".join(sorted(missing))
        )

    return datasets


def _apply_column_mapping(
    df: pd.DataFrame,
    mapping: Mapping[str, str],
    target_columns: Sequence[str],
) -> pd.DataFrame:
    if not mapping:
        return df

    working = df.copy()
    rename_map: Dict[str, str] = {}
    for target_column, source_column in mapping.items():
        if source_column is None or target_column == source_column:
            continue
        source_column_normalised = str(source_column).strip()
        if not source_column_normalised:
            continue
        if source_column_normalised in working.columns:
            rename_map[source_column_normalised] = target_column
        else:
            logger.debug(
                "Schema mapping skipped: source column '%s' not present for target '%s'",
                source_column_normalised,
                target_column,
            )

    if rename_map:
        working.rename(columns=rename_map, inplace=True)

    for column in target_columns:
        if column not in working.columns:
            working[column] = None

    return working.reindex(columns=target_columns)


def load(df: pd.DataFrame, table: str, settings: ETLSettings) -> Dict[str, str | int | None]:
    """Persist the dataframe to disk and optionally upload it to S3."""

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"{table}_{timestamp}.csv"
    local_connector = LocalFileConnector(settings.processed_dir)
    local_path = local_connector.write(table, df, filename)

    s3_uri: str | None = None
    if settings.enable_s3 and settings.s3_bucket:
        try:
            s3 = S3Connector(
                settings.s3_bucket,
                prefix=settings.s3_prefix,
                region_name=settings.aws_region,
                access_key=settings.aws_access_key_id,
                secret_key=settings.aws_secret_access_key,
                session_token=settings.aws_session_token,
            )
            s3_uri = s3.write(table, local_path)
        except StorageError as exc:
            raise ETLError(str(exc)) from exc

    return {
        "table": table,
        "row_count": int(df.shape[0]),
        "local_path": str(local_path),
        "s3_uri": s3_uri,
    }


def run_pipeline(
    table: str,
    settings: ETLSettings,
    *,
    manifest: ETLManifest | None = None,
    column_mappings: Mapping[str, Mapping[str, str]] | None = None,
) -> Dict[str, str | int | None]:
    """High-level helper to execute the raw JSON ? CSV ? S3 pipeline."""

    file_paths = extract(settings)
    catalog = get_schema_catalog(settings)
    df = transform(
        file_paths,
        table=table,
        max_records=settings.max_records,
        catalog=catalog,
        column_mapping=(column_mappings or {}).get(table),
    )
    return load(df, table=table, settings=settings)


def run_pipeline_all(
    settings: ETLSettings,
    *,
    manifest: ETLManifest | None = None,
    column_mappings: Mapping[str, Mapping[str, str]] | None = None,
) -> Dict[str, Dict[str, str | int | None]]:
    """Execute the pipeline for every supported table in one pass."""

    file_paths = extract(settings)
    catalog = get_schema_catalog(settings)
    datasets = transform_all(
        file_paths,
        max_records=settings.max_records,
        catalog=catalog,
        require_all_tables=True,
        column_mappings=column_mappings,
    )

    results: Dict[str, Dict[str, str | int | None]] = {}
    for table, dataframe in datasets.items():
        results[table] = load(dataframe, table=table, settings=settings)
    return results


def _resource_to_record(resource: dict, table: str) -> dict | None:
    if table == "patients":
        address_obj = (resource.get("address") or [{}])[0]
        address_line = address_obj.get("line") or []
        full_address = " ".join(
            [*address_line, address_obj.get("city", ""), address_obj.get("state", ""), address_obj.get("postalCode", ""), address_obj.get("country", "")]
        ).strip()

        ssn = None
        for ext in resource.get("extension", []):
            if "SocialSecurityNumber" in ext.get("url", ""):
                ssn = ext.get("valueString")

        return {
            "id": resource.get("id"),
            "birthdate": resource.get("birthDate"),
            "deathdate": resource.get("deceasedDateTime"),
            "ssn": ssn,
            "drivers": None,
            "passport": None,
            "prefix": _safe_get(resource, ["name", 0, "prefix", 0]),
            "first": _safe_get(resource, ["name", 0, "given", 0]),
            "last": _safe_get(resource, ["name", 0, "family"]),
            "suffix": _safe_get(resource, ["name", 0, "suffix", 0]),
            "maiden": None,
            "marital": _safe_get(resource, ["maritalStatus", "text"]),
            "race": None,
            "ethnicity": None,
            "gender": resource.get("gender"),
            "birthplace": _safe_get(resource, ["extension", 2, "valueAddress", "city"]),
            "address": full_address,
        }

    if table == "encounters":
        return {
            "id": resource.get("id"),
            "date": _safe_get(resource, ["period", "start"]) or _safe_get(resource, ["meta", "lastUpdated"]),
            "patient": _get_ref_id(_safe_get(resource, ["subject", "reference"])),
            "code": _safe_get(resource, ["type", 0, "coding", 0, "code"]) or _safe_get(resource, ["class", "code"]),
            "description": _safe_get(resource, ["type", 0, "text"]) or _safe_get(resource, ["class", "display"]),
            "reasoncode": _safe_get(resource, ["reasonCode", 0, "coding", 0, "code"]),
            "reasondescription": _safe_get(resource, ["reasonCode", 0, "text"]),
        }

    if table == "conditions":
        return {
            "start": resource.get("onsetDateTime"),
            "stop": resource.get("abatementDateTime") or resource.get("assertedDate"),
            "patient": _get_ref_id(_safe_get(resource, ["subject", "reference"])),
            "encounter": _get_ref_id(
                _safe_get(resource, ["encounter", "reference"]) or _safe_get(resource, ["context", "reference"])
            ),
            "code": _safe_get(resource, ["code", "coding", 0, "code"]),
            "description": _safe_get(resource, ["code", "text"]),
        }

    if table == "observations":
        value, units = None, None
        if "valueQuantity" in resource:
            value = _safe_get(resource, ["valueQuantity", "value"])
            units = _safe_get(resource, ["valueQuantity", "unit"])
        elif "valueCodeableConcept" in resource:
            value = _safe_get(resource, ["valueCodeableConcept", "text"])

        return {
            "date": resource.get("effectiveDateTime"),
            "patient": _get_ref_id(_safe_get(resource, ["subject", "reference"])),
            "encounter": _get_ref_id(
                _safe_get(resource, ["encounter", "reference"]) or _safe_get(resource, ["context", "reference"])
            ),
            "code": _safe_get(resource, ["code", "coding", 0, "code"]),
            "description": _safe_get(resource, ["code", "text"]),
            "value": value,
            "units": units,
        }

    if table == "medications":
        return {
            "start": resource.get("authoredOn") or _safe_get(resource, ["dispenseRequest", "validityPeriod", "start"]),
            "stop": _safe_get(resource, ["dispenseRequest", "validityPeriod", "end"]),
            "patient": _get_ref_id(_safe_get(resource, ["subject", "reference"])),
            "encounter": _get_ref_id(
                _safe_get(resource, ["encounter", "reference"]) or _safe_get(resource, ["context", "reference"])
            ),
            "code": _safe_get(resource, ["medicationCodeableConcept", "coding", 0, "code"]),
            "description": _safe_get(resource, ["medicationCodeableConcept", "text"]),
            "reasoncode": _safe_get(resource, ["reasonCode", 0, "coding", 0, "code"]),
            "reasondescription": _safe_get(resource, ["reasonCode", 0, "text"]),
        }

    if table == "procedures":
        return {
            "date": resource.get("performedDateTime"),
            "patient": _get_ref_id(_safe_get(resource, ["subject", "reference"])),
            "encounter": _get_ref_id(
                _safe_get(resource, ["encounter", "reference"]) or _safe_get(resource, ["context", "reference"])
            ),
            "code": _safe_get(resource, ["code", "coding", 0, "code"]),
            "description": _safe_get(resource, ["code", "text"]),
            "reasoncode": _safe_get(resource, ["reasonCode", 0, "coding", 0, "code"]),
            "reasondescription": _safe_get(resource, ["reasonCode", 0, "text"]),
        }

    return None


def _get_ref_id(reference: str | None) -> str | None:
    if not reference:
        return None
    return reference.split("/")[-1].split(":")[-1]


def _safe_get(obj: dict | list | None, path: List[object]) -> str | None:
    current = obj
    for key in path:
        if isinstance(key, int):
            if isinstance(current, list) and 0 <= key < len(current):
                current = current[key]
            else:
                return None
        else:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
    return current


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the healthcare ETL pipeline.")
    parser.add_argument(
        "--table",
        choices=SchemaCatalog.default().table_names,
        help="Run the pipeline for a single table.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run the pipeline for all supported tables in one shot.",
    )
    parser.add_argument("--input-dir", help="Override the raw input directory.")
    parser.add_argument("--output-dir", help="Override the processed output directory.")
    parser.add_argument("--schema-config", help="Path to a schema configuration JSON file.")
    parser.add_argument(
        "--max-records",
        type=int,
        help="Limit the number of records per table (0 = no cap).",
    )
    parser.add_argument("--disable-s3", action="store_true", help="Disable S3 uploads for this run.")
    parser.add_argument("--s3-bucket", help="Override the target S3 bucket name.")
    parser.add_argument("--s3-prefix", help="Override the S3 key prefix.")
    parser.add_argument("--s3-region", help="Override the AWS region for S3 uploads.")
    return parser.parse_args(argv)


def _apply_cli_overrides(settings: ETLSettings, args: argparse.Namespace) -> ETLSettings:
    overrides: Dict[str, object] = {}

    if args.input_dir:
        overrides["raw_dir"] = args.input_dir
    if args.output_dir:
        overrides["processed_dir"] = args.output_dir
    if args.schema_config:
        overrides["schema_config_path"] = args.schema_config
    if args.max_records is not None:
        overrides["max_records"] = max(args.max_records, 0)
    if args.s3_bucket:
        overrides["s3_bucket"] = args.s3_bucket
    if args.s3_prefix is not None:
        overrides["s3_prefix"] = args.s3_prefix
    if args.s3_region:
        overrides["aws_region"] = args.s3_region
    if args.disable_s3:
        overrides["enable_s3"] = False
    elif args.s3_bucket or args.s3_prefix or args.s3_region:
        # Assume uploads should happen when overrides are provided.
        overrides.setdefault("enable_s3", True)

    if overrides:
        settings = replace(settings, **overrides)
    return settings


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    base_settings = get_settings().etl
    resolved_settings, manifest = resolve_etl_settings(base_settings)
    settings = _apply_cli_overrides(resolved_settings, args)

    if args.all:
        results = run_pipeline_all(settings, manifest=manifest)
        for table, metadata in results.items():
            logger.info("Pipeline emitted %s rows to %s", metadata["row_count"], metadata["local_path"])
        return

    if args.table:
        result = run_pipeline(args.table, settings, manifest=manifest)
        logger.info("Pipeline emitted %s rows to %s", result["row_count"], result["local_path"])
        return

    raise SystemExit("Specify --table <name> or --all")


if __name__ == "__main__":
    main()
