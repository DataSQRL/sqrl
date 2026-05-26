# DuckDB Engine Configuration

DuckDB is a vectorized database query engine that excels at analytical queries and can read Iceberg tables efficiently.

## Configuration Options

| Key                    | Type        | Default          | Description                                                                    |
|------------------------|-------------|------------------|--------------------------------------------------------------------------------|
| `url`                  | **string**  | `"jdbc:duckdb:"` | Full JDBC URL for the database connection                                      |
| `use-disk-cache`       | **boolean** | `false`          | Install and load `cache_httpfs` extension                                      |
| `use-version-guessing` | **boolean** | `false`          | Sets `unsafe_enable_version_guessing` flag to be able to read uncommitted data |
| `use-credential-chain` | **boolean** | `false`          | Load the `aws` extension and create an S3 secret backed by the AWS credential provider chain |

## Example Configuration

```json
{
  "engines": {
    "duckdb": {
      "url": "jdbc:duckdb:",
      "use-disk-cache": true,
      "use-version-guessing": true
    }
  }
}
```

## Usage Notes

- Ideal for local development and testing of analytical workloads
- Excellent performance on analytical queries with vectorized execution
- Can read Iceberg tables directly without additional infrastructure
- Enable `use-credential-chain` when reading S3-backed Iceberg from an environment that supplies credentials through the AWS provider chain rather than static keys — e.g. EKS IRSA, where the pod only has a projected web-identity token. Plain `httpfs` does not perform the web-identity exchange, so without this flag S3 reads fail with `HTTP 403`.
- Supports both in-memory and persistent database modes
- Perfect for prototyping before deploying to cloud query engines like Snowflake
- Lightweight alternative to larger analytical databases
