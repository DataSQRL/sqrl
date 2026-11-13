# Iceberg Query Engine Configurations

Apache Iceberg stores analytic data but does not execute queries. Enable an Iceberg query engine alongside `iceberg` to create and query Iceberg tables.

## Full Query Engines

Full query engines are integrated with the DataSQRL server and can execute generated API queries against Iceberg tables.

### DuckDB (`duckdb`)

DuckDB is a vectorized database query engine that excels at analytical queries and can read Iceberg tables efficiently.

#### Configuration Options

| Key                            | Type        | Default          | Description                                                                                                                                                                                                                                                                                                                                            |
|--------------------------------|-------------|------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `url`                          | **string**  | `"jdbc:duckdb:"` | Full JDBC URL for the database connection                                                                                                                                                                                                                                                                                                              |
| `pool-size`                    | **integer** | `1`              | Number of queries the server runs on DuckDB at the same time, per API version; other queries wait. Each API version has its own DuckDB database with its own `memory-limit`, so a server with two API versions can run twice as many queries and use twice the memory. With `use-credential-chain`, a value above 1 makes queries fail when the credentials expire, see the usage notes |
| `memory-limit`                 | **string**  | -                | Sets DuckDB's `memory_limit`, for example `"8GB"`                                                                                                                                                                                                                                                                                                      |
| `temp-directory`               | **string**  | `sqrl-duckdb` in the JVM temp directory | Directory DuckDB writes spill files to when a query needs more than `memory-limit`. Must be writable. The server image default is `/tmp/sqrl-duckdb`                                                                                                                                                                                                   |
| `max-temp-directory-size`      | **string**  | `"4GB"`          | Disk space DuckDB may use for spill files. A query that needs more fails with an Out of Memory error                                                                                                                                                                                                                                                   |
| `use-disk-cache`               | **boolean** | `false`          | Loads the `cache_httpfs` extension (requires `DUCKDB_EXTENSIONS_DIR`, see below). New Iceberg commits then reach the server late, see the usage notes                                                                                                                                                                                                  |
| `use-version-guessing`         | **boolean** | `false`          | Sets `unsafe_enable_version_guessing` flag to be able to read uncommitted data (requires `DUCKDB_EXTENSIONS_DIR`, see below)                                                                                                                                                                                                                           |
| `use-credential-chain`         | **boolean** | `false`          | Loads the `aws` extension and creates an S3 secret from the AWS credential provider chain: environment variables, profiles, or the EKS IRSA web-identity token. DuckDB refreshes the credentials when S3 rejects them as expired and retries the request (with `pool-size` above 1, see the usage notes). Requires `DUCKDB_EXTENSIONS_DIR`             |
| `s3-endpoint`                  | **string**  | -                | URL of an S3-compatible store such as S3Mock, for example `"http://localhost:9090"`. The `use-credential-chain` secret then uses this endpoint with path-style URLs, and with SSL for `https` URLs. Requires `use-credential-chain`                                                                                                                    |
| `scan-cte-cardinality-divisor` | **integer** | `1`              | Controls when a query that scans the same Iceberg table multiple times reads it once into a shared CTE. The scan is shared when the number of scans exceeds `max((log2(row count) - 20) / divisor, 1)`; a larger divisor shares scans more eagerly. Requires a known row count (estimated or set via the `row_count` hint). Must be a positive integer |

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

:::note
The DataSQRL server only loads DuckDB extensions (`iceberg`, `httpfs`, `aws`, and `cache_httpfs`) and applies `use-version-guessing` and `use-credential-chain` when the `DUCKDB_EXTENSIONS_DIR` environment variable points to a directory with the preinstalled extensions. The official `datasqrl/cmd` and `datasqrl/sqrl-server` Docker images set it. When running the server elsewhere without it, the server logs a warning and silently skips these settings.
:::

#### Usage Notes

- Ideal for local development and testing of analytical workloads
- Excellent performance on analytical queries with vectorized execution
- Can read Iceberg tables directly without additional infrastructure
- Supports both in-memory and persistent database modes
- Perfect for prototyping before deploying to cloud query engines like Snowflake
- Lightweight alternative to larger analytical databases
- With `use-disk-cache`, the server does not see new Iceberg commits right away. With `use-version-guessing`, a commit shows up when the cached folder listing expires, after up to 30 minutes. A table with a `version-hint.text` on S3 stays on the snapshot the server read first for as long as the hint file is in the extension's cache directory, `/tmp/duckdb_cache_httpfs_cache`
- With `use-credential-chain`, the server fails to start if the chain finds no credentials
- `pool-size` is 1 by default because of a DuckDB bug, [duckdb-httpfs#462](https://github.com/duckdb/duckdb-httpfs/issues/462): with `use-credential-chain` and a `pool-size` above 1, queries can fail when the AWS credentials expire, which happens once per credential lifetime (an hour by default with EKS IRSA). Every query that gets the expiry error from S3 refreshes the secret, but only one refresh can win: the others fail with `Exception thrown while trying to refresh secret "sqrl_s3_credential_chain"` and `Catalog write-write conflict on alter with "sqrl_s3_credential_chain"`. In a test with four simultaneous queries, three failed at each expiry. The winning query stores the new credentials when it finishes, so requests that start after that work, and a request retried before then fails again. If you raise `pool-size` with `use-credential-chain`, retry failed requests after a delay. Without `use-credential-chain`, DuckDB does not refresh credentials and a higher `pool-size` does not hit this bug. [duckdb#24804](https://github.com/duckdb/duckdb/pull/24804) proposes a fix
- Without `use-credential-chain`, DuckDB 2.0 still signs S3 requests with AWS credentials it finds in the environment, but never refreshes them
- For S3Mock (`adobe/s3mock`), set `"use-credential-chain": true` and `"s3-endpoint": "http://s3mock:9090"`, and give the server any keys in `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. S3Mock does not check them, but without keys the credential chain finds nothing and the server fails to start. `AWS_REGION` is optional

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

Spark SQL generates Spark SQL definitions for Iceberg tables. Enable it by adding `"sparksql"` to `enabled-engines`.

#### Configuration Options

| Key             | Type       | Default   | Description                                                  |
|-----------------|------------|-----------|--------------------------------------------------------------|
| `view-catalog`  | **string** | -         | Catalog that contains generated views                        |
| `view-database` | **string** | `default` | Database that contains generated views when a catalog is set |

When configured, generated view names are qualified as `catalog.database.view`. Setting only `view-database` qualifies views as `database.view` in the current catalog.
Setting `view-catalog` without `view-database` uses Spark SQL's `default` database.

### Redshift (`redshift`)

Redshift generates Amazon Redshift SQL definitions for Iceberg tables. Enable it by adding `"redshift"` to `enabled-engines`.

#### Configuration Options

| Key             | Type       | Default  | Description                                                   |
|-----------------|------------|----------|---------------------------------------------------------------|
| `view-database` | **string** | -        | Database that contains generated views                        |
| `view-schema`   | **string** | `public` | Schema that contains generated views when a database is set   |

When configured, generated view names are qualified as `database.schema.view`. Setting only `view-schema` qualifies views as `schema.view` in the current database.
Setting `view-database` without `view-schema` uses Redshift's `public` schema.

### Trino (`trino`)

Trino generates Trino SQL definitions for Iceberg tables. Enable it by adding `"trino"` to `enabled-engines`.

#### Configuration Options

| Key            | Type       | Default  | Description                                                |
|----------------|------------|----------|------------------------------------------------------------|
| `view-catalog` | **string** | -        | Catalog that contains generated views                      |
| `view-schema`  | **string** | `public` | Schema that contains generated views when a catalog is set |

When configured, generated view names are qualified as `catalog.schema.view`. Setting only `view-schema` qualifies views as `schema.view` in the current catalog.
Setting `view-catalog` without `view-schema` uses Trino's `public` schema.
