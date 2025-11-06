# ETL Schema Configuration

The ETL pipeline reads a schema catalog that maps FHIR resource types to tabular outputs and lists the columns to emit. By default, the catalog lives at `config/etl_schema.json`, but you can override the location with:

- Environment variable `ETL_SCHEMA_CONFIG`
- CLI flag `--schema-config <path>`

## File Structure

```json
{
  "tables": {
    "patients": {
      "columns": ["id", "birthdate", "..."],
      "resource_types": ["Patient"]
    },
    "encounters": {
      "columns": ["id", "date", "patient", "..."],
      "resource_types": ["Encounter"]
    }
  }
}
```

- `columns`: ordered list that becomes the CSV header and downstream schema.
- `resource_types`: resource identifiers that will be routed to the table. Multiple resource types can map to the same table (e.g., `MedicationRequest`, `MedicationOrder` → `medications`).

## Extending the Catalog

1. Copy `config/etl_schema.json` and add/modify table definitions.
2. Point the pipeline to the new file with `--schema-config` or `ETL_SCHEMA_CONFIG`.
3. Update the transformation logic (e.g., `_resource_to_record`) to produce records for new tables. The agent retrieves the catalog documentation from `rag_docs` so make sure the doc stays in sync.

## Agent Usage

- The planner retrieves the schema catalog docs to understand available tables.
- The ETL directive guardrail validates that the target table exists in the catalog.
- Future connectors/tools can load the same catalog to align dataframes with warehouse schemas.
