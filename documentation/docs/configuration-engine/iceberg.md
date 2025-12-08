# Iceberg Engine Configuration

Apache Iceberg is an analytic database format that provides ACID transactions, schema evolution, and time travel capabilities for large analytic datasets.

## Configuration Options

Iceberg is used as a *table-format* engine and must be paired with a query engine such as Snowflake, or DuckDB for query access.

## Basic Configuration

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

## Usage Notes

- Iceberg serves as a storage format, not a query engine
- Must be combined with compatible query engines:
  - **DuckDB**: For local analytics and testing
  - **Snowflake**: For cloud-scale analytics  
- Provides schema evolution capabilities for long-running pipelines
- Supports time travel queries for historical data analysis
- Optimized for large-scale analytical workloads
