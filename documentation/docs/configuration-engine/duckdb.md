# DuckDB Engine Configuration

DuckDB is a vectorized database query engine that excels at analytical queries and can read Iceberg tables efficiently.

## Configuration Options

| Key                    | Type        | Default          | Description                                                                    |
|------------------------|-------------|------------------|--------------------------------------------------------------------------------|
| `url`                  | **string**  | `"jdbc:duckdb:"` | Full JDBC URL for the database connection                                      |
| `memory-limit`         | **string**  | -                | Sets DuckDB's `memory_limit`, for example `"8GB"`                              |
| `use-disk-cache`       | **boolean** | `false`          | Install and load `cache_httpfs` extension                                      |
| `use-version-guessing` | **boolean** | `false`          | Sets `unsafe_enable_version_guessing` flag to be able to read uncommitted data |
| `use-credential-chain` | **boolean** | `false`          | Load the `aws` extension and create an S3 secret backed by the AWS credential provider chain. Credentials are **not refreshed** — see below |

## Example Configuration

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

## Usage Notes

- Ideal for local development and testing of analytical workloads
- Excellent performance on analytical queries with vectorized execution
- Can read Iceberg tables directly without additional infrastructure
- Enable `use-credential-chain` when reading S3-backed Iceberg from an environment that supplies credentials through the AWS provider chain rather than static keys — e.g. EKS IRSA, where the pod only has a projected web-identity token. Plain `httpfs` does not perform the web-identity exchange, so without this flag S3 reads fail with `HTTP 403`.
- Supports both in-memory and persistent database modes
- Perfect for prototyping before deploying to cloud query engines like Snowflake
- Lightweight alternative to larger analytical databases

### Credential refresh is not supported

The S3 secret is created by the connection init SQL, so credentials are resolved once when a DuckDB
connection is opened and are **never refreshed for the life of that connection**. Provider-chain
credentials are usually temporary — an IRSA web-identity session typically lasts an hour — so a
long-running server can start failing S3 reads with `HTTP 403` once they expire, and keep failing
until the pool opens a new physical connection and re-runs the init SQL.

This cannot be fixed from SQRL alone: DuckDB's `httpfs` does not refresh credentials at every call
site that needs them ([duckdb/duckdb-httpfs#165](https://github.com/duckdb/duckdb-httpfs/pull/165),
still unmerged). Until that lands, treat `use-credential-chain` as suitable for short-lived queries
and for workloads that tolerate connection recycling, and expect failures wherever a single
connection must serve reads for longer than the credential lifetime.
