# Iceberg Engine Configuration

Apache Iceberg is an analytic database format that provides ACID transactions, schema evolution, and time travel capabilities for large analytic datasets.

## Configuration Options

Iceberg is used as a *table-format* engine and must be paired with a query engine such as Snowflake, or DuckDB for query access.

## Basic Configuration

```json
{
  "engines": {
    "iceberg": {
      "config": {
        // Iceberg-specific configuration options
      }
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