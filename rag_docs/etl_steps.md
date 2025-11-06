# ETL Pipeline Steps

## 1. extract()
- Read raw FHIR JSON bundles from `ETL_RAW_DIR` (defaults to `data/raw`).
- Validate the target table is one of: `patients`, `encounters`, `conditions`, `observations`, `medications`, `procedures`.
- Collect every `.json` file in the directory; raise an `ETLError` if nothing is found.

## 2. transform_all()
- Load each bundle and iterate through `entry[*].resource`.
- Map the resource type to the target table using the schema catalog (`SchemaCatalog.table_for_resource`).
- Flatten nested fields to match the curated schema columns (see `_resource_to_record`).
- Drop duplicate records and truncate to `ETL_MAX_RECORDS` when configured.
- Raise an `ETLError` if a requested table emits no records when `require_all_tables=True`.

## 3. load()
- Use `LocalFileConnector` to write the dataframe to `ETL_PROCESSED_DIR/<table>/<table>_<timestamp>.csv` (directory created on demand).
- Optional connectors (e.g., `S3Connector`) can publish the artifact to cloud storage (`s3://<bucket>/<prefix>/<table>/<filename>`).
- Return the local path, S3 URI (if uploaded), and row count to the caller.

## 4. db_loader (optional)
- When `ETL_ENABLE_DB_LOAD` is true, call `db_loader.load_table_from_csv()` to append or replace records in the downstream warehouse.
- Truncate behavior is controlled via `ETL_DB_TRUNCATE`; batching via `ETL_DB_CHUNKSIZE`.
- Load results (rows inserted) are surfaced in the agent response for observability.

## Error Handling
- `ETLError` surfaces missing directories, invalid tables, JSON parsing, empty outputs, or S3 upload failures.
- The agent retry loop uses the error message to request a corrected directive from the LLM.

## Operational Notes
- Keep AWS credentials in environment variables or ~/.aws/credentials.
- Disable S3 uploads by setting `ETL_ENABLE_S3=false` or CLI `--disable-s3` for local testing.
- The processed directory is safe to clear between runs; source files remain untouched.
- Override inputs/outputs at runtime with CLI flags (`--input-dir`, `--output-dir`, `--schema-config`, `--max-records`).
