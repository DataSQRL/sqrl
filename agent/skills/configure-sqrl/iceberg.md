# Iceberg Engine Configuration

Iceberg is used as a *table-format* engine and must be paired with at least one query engine for query access. `duckdb` is the only full query engine integrated with the DataSQRL server. `snowflake`, `sparksql`, `redshift`, and `trino` are shallow query engines: they generate engine-specific Iceberg definitions and SQL, but cannot execute generated API queries. Pair a shallow engine with DuckDB when the project also exposes an API. Read [iceberg-query.md](iceberg-query.md) to select and configure one.

Use the exact enabled-engine names: `duckdb`, `snowflake`, `sparksql` (not `spark`), `redshift`, and `trino`. `athena` is not a supported SQRL engine.

Since Iceberg is not a standalone data system but a data format, the configuration for Iceberg is managed through the shared `iceberg` connector:

```json5
{
  "connectors": {
    "iceberg": {
      "warehouse": "iceberg-data",      // path the Iceberg table data is written to
      "catalog-name": "default_catalog" // the name of the catalog
    }
  }
}
```

Read [connector templates](connector-templates.md) before adjusting the Iceberg connector templates.
