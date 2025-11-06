# ETL Pipeline Overview

- **Raw directory**: `data/raw` (default). Override via `ETL_RAW_DIR` or the CLI `--input-dir`.
- **Processed directory**: `data/processed/etl` (default). Override via `ETL_PROCESSED_DIR` or `--output-dir`.
- **Schema catalog**: `config/etl_schema.json` documents table columns and the resource → table mapping. Override with `ETL_SCHEMA_CONFIG` or `--schema-config`.
- **Supported tables** (default): patients, encounters, conditions, observations, medications, procedures. Each CSV matches the curated Postgres schema.
- **S3 layout**: outputs upload to `s3://<bucket>/<prefix>/<table>/<timestamp>.csv` when S3 is enabled.
- **Primary steps**: `extract()` gathers JSON bundles; `transform_all()` flattens them according to the schema catalog; `load()` writes the CSV locally / S3; optional `db_loader.load_table_from_csv()` pushes the curated file into the warehouse.
- **Environment settings**: `ETL_RAW_DIR`, `ETL_PROCESSED_DIR`, `ETL_SCHEMA_CONFIG`, `S3_BUCKET`, `S3_PREFIX`, `AWS_REGION`, `ETL_ENABLE_S3`, `ETL_MAX_RECORDS`.
- Keep AWS credentials in the environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN`) or an instance role.

## Connectors

- `LocalFileConnector` writes CSV outputs under `processed/<table>/`.
- `S3Connector` uploads the local artifact to the configured bucket/prefix. Disable via `ETL_ENABLE_S3=false` or CLI `--disable-s3`.
- Additional connectors can subclass the same pattern (e.g., Redshift, Google Sheets) and be invoked from `load()`.

## CLI Usage

```
python -m app.etl.json_to_s3 --all \
  --input-dir data/raw \
  --output-dir data/processed/etl \
  --schema-config config/etl_schema.json \
  --s3-bucket my-bucket \
  --s3-prefix healthcare/exports \
  --max-records 1000
```

- `--table <name>` runs a single-table export.
- `--disable-s3` skips uploads regardless of environment configuration.
- CLI overrides are applied at runtime so the agent can parameterise pipelines per user request.
