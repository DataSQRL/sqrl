# Iceberg Query Engine Configurations

Apache Iceberg stores analytic data but does not execute queries. Enable an Iceberg query engine alongside `iceberg` to create and query Iceberg tables.

## Full Query Engines

Full query engines are integrated with the DataSQRL server and can execute generated API queries against Iceberg tables.

### DuckDB (`duckdb`)

DuckDB is a vectorized database query engine that excels at analytical queries and can read Iceberg tables efficiently.

#### Configuration Options

| Key                    | Type        | Default          | Description                                                                    |
|------------------------|-------------|------------------|--------------------------------------------------------------------------------|
| `url`                  | **string**  | `"jdbc:duckdb:"` | Full JDBC URL for the database connection                                      |
| `memory-limit`         | **string**  | -                | Sets DuckDB's `memory_limit`, for example `"8GB"`                              |
| `use-disk-cache`       | **boolean** | `false`          | Install and load `cache_httpfs` extension                                      |
| `use-version-guessing` | **boolean** | `false`          | Sets `unsafe_enable_version_guessing` flag to be able to read uncommitted data |

#### Example Configuration

```json
{
  "engines": {
    "duckdb": {
      "url": "jdbc:duckdb:",
      "memory-limit": "4GB",
      "use-disk-cache": true,
      "use-version-guessing": true
    }
  }
}
```

#### Usage Notes

- Ideal for local development and testing of analytical workloads
- Excellent performance on analytical queries with vectorized execution
- Can read Iceberg tables directly without additional infrastructure
- Supports both in-memory and persistent database modes
- Perfect for prototyping before deploying to cloud query engines like Snowflake
- Lightweight alternative to larger analytical databases

## Shallow Query Engines

Shallow query engines generate engine-specific Iceberg table definitions and query SQL, but are not integrated with the DataSQRL server.
They cannot execute generated API queries. When an API server is enabled, pair a shallow query engine with the full DuckDB query engine.

### Snowflake (`snowflake`)

Snowflake is a cloud-based analytic database query engine that can read Iceberg tables and provide enterprise-scale analytical capabilities.

#### Configuration Options

| Key               | Type       | Default | Description                         |
|-------------------|------------|---------|-------------------------------------|
| `catalog-name`    | **string** | -       | Glue catalog name for metadata      |
| `external-volume` | **string** | -       | Snowflake external volume name      |
| `url`             | **string** | -       | Full JDBC URL including auth params |

#### Example Configuration

```json
{
  "engines": {
    "snowflake": {
      "catalog-name": "my-glue-catalog",
      "external-volume": "my-external-volume",
      "url": "jdbc:snowflake://account.snowflakecomputing.com/?user=username&password=password&warehouse=warehouse&db=database&schema=schema"
    }
  }
}
```

#### Usage Notes

- Requires all three configuration parameters
- Designed for large-scale analytical workloads in the cloud
- Integrates with AWS Glue for metadata management
- Uses external volumes for accessing Iceberg data
- Authentication parameters should be included in the JDBC URL
- For local development, consider using DuckDB as a substitute
- Provides enterprise features like data sharing and governance

### Spark SQL (`sparksql`)

Spark SQL generates Spark SQL definitions for Iceberg tables. Enable it by adding `"sparksql"` to `enabled-engines`. It has no engine-specific configuration options.

### Redshift (`redshift`)

Redshift generates Amazon Redshift SQL definitions for Iceberg tables. Enable it by adding `"redshift"` to `enabled-engines`. It has no engine-specific configuration options.
