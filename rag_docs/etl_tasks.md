# ETL Tasks and Examples

## Pipeline: Raw JSON -> CSV -> S3
- The agent selects a target table and executes the helper in pp/etl/json_to_s3.py.
- The output CSV is written to data/processed/etl/<table>/<table>_<timestamp>.csv.
- When S3 is enabled the same file is uploaded to the configured bucket/prefix.
- Errors (missing files, unsupported table, S3 failure) bubble up through the repair loop so the LLM can adjust or stop.

## Example Prompts
- "Transform raw patients JSON into CSV and upload it to S3."
- "Ingest the latest encounters JSON bundle and push the structured CSV to our processed bucket."
- "Run the ETL pipeline for medications."

The agent responds with the table name, row count, local path, and S3 URI (if uploaded).
