# DuckDB Engine Configuration

DuckDB is a vectorized database query engine that excels at analytical queries and can read Iceberg tables efficiently.

## Configuration Options

| Key   | Type       | Default          | Description    |
|-------|------------|------------------|----------------|
| `url` | **string** | `"jdbc:duckdb:"` | Full JDBC URL for database connection |

## Example Configuration

```json
{
  "engines": {
    "duckdb": {
      "url": "jdbc:duckdb:"
    }
  }
}
```

## Usage Notes

- Ideal for local development and testing of analytical workloads
- Excellent performance on analytical queries with vectorized execution
- Can read Iceberg tables directly without additional infrastructure
- Supports both in-memory and persistent database modes
- Perfect for prototyping before deploying to cloud query engines like Snowflake
- Lightweight alternative to larger analytical databases