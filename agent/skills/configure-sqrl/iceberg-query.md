# Iceberg Query Engine Configuration

Apache Iceberg stores analytic data but does not run queries. Enable at least one Iceberg query engine next to `iceberg`, so the pipeline can create the Iceberg tables and query them. Read [iceberg.md](iceberg.md) for the Iceberg engine itself.

## Two Kinds of Query Engines

| Kind | Engines | What it does | Serves API queries and tests? |
|------|---------|--------------|-------------------------------|
| **Full** | `duckdb` | Runs inside the pipeline and is connected to the DataSQRL server. The server runs the generated API queries and the test queries against the Iceberg tables through it. | Yes |
| **Shallow** | `snowflake`, `sparksql`, `redshift`, `trino` | Only generates SQL: the table definitions that register the Iceberg tables in that engine, and query SQL. Run that SQL in the selected external query engine as a separate deployment step. They are not integrated with the DataSQRL server. They cannot execute generated API queries.  | No |

Pick the engines with these rules:

* The project serves an API (`vertx` is enabled) over Iceberg tables, then enable `duckdb`. It is the only full engine, so it is the only engine the server can query.
* The project needs to query Iceberg tables through Snowflake, Spark SQL, Redshift or Trino. Enable that shallow engine. Keep `duckdb` next to it when the project also serves an API, because a shallow engine cannot answer API queries.
* If the requirements specify neither an API nor an external query engine, enable `duckdb`, so the tables can be queried in tests and local runs.
* The project needs to query the tables through several external query engines. Enable each required shallow engine. Each engine gets its own generated SQL files.
* When these query engines are used, add `iceberg` in `enabled-engines` in the configuration file.

## Engine Set Configuration per Environment

`enabled-engines` is an array, and an overlay replaces the whole array. Keep `duckdb` in the shared base, so the test, local and dev environments compile and run their test tables and test queries. When production uses Snowflake, Spark SQL, Redshift or Trino to query the tables instead of DuckDB, list the full production set in the `-prod` overlay together with the shallow engine's `engines.<engine>` block, because `engines` must not name an engine that is not enabled in that environment.

Examples: 
**`<subproject>-shared-package.json`**, the base: DuckDB runs the tests and local runs.

```json
{
  "enabled-engines": ["flink", "iceberg", "duckdb"]
}
```

**`<subproject>-prod-package.json`**, the production overlay: Snowflake might be the only query engine and there is no API, so the array drops `duckdb`.

```json
{
  "enabled-engines": ["flink", "iceberg", "snowflake"],
  "engines": {
    "snowflake": {
      "catalog-name": "my-glue-catalog",
      "external-volume": "my-external-volume",
      "url": "${SNOWFLAKE_JDBC_URL}"
    }
  }
}
```

When production keeps the API, keep `duckdb` in the production array and add the shallow engine to it: `["flink", "iceberg", "duckdb", "snowflake", "vertx"]`.

## Full Query Engines

### DuckDB (`duckdb`)

DuckDB is a vectorized query engine that reads Iceberg tables directly. It runs inside the pipeline and needs no extra infrastructure, which makes it the engine for local development, tests and API serving over a data lake.

| Key                    | Type        | Default          | Description                                                                    |
|------------------------|-------------|------------------|--------------------------------------------------------------------------------|
| `url`                  | **string**  | `"jdbc:duckdb:"` | Full JDBC URL for the database connection                                      |
| `memory-limit`         | **string**  | -                | Sets DuckDB's `memory_limit`, for example `"8GB"`                              |
| `use-disk-cache`       | **boolean** | `false`          | Install and load `cache_httpfs` extension                                      |
| `use-version-guessing` | **boolean** | `false`          | Sets `unsafe_enable_version_guessing` flag to be able to read uncommitted data |

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



Set `memory-limit` when DuckDB runs next to Flink on the same instance and both compete for memory. Read the `.mem-headroom` size qualifier in [cloud-deployment.md](cloud-deployment.md) for that case.

## Shallow Query Engines

Shallow query engines generate engine-specific Iceberg table definitions and query SQL, but are not integrated with the DataSQRL server. They cannot execute generated API queries

Enable a shallow engine by adding its name to `enabled-engines`. Add an `engines.<name>` block only when the engine has required keys (Snowflake) or the generated views need catalog, database or schema prefixes (Spark SQL, Redshift, Trino).

### Snowflake (`snowflake`)

Snowflake reads the Iceberg tables through an AWS Glue catalog and a Snowflake external volume. All three keys are required.

| Key               | Type       | Default | Description                         |
|-------------------|------------|---------|-------------------------------------|
| `catalog-name`    | **string** | -       | Glue catalog name for metadata      |
| `external-volume` | **string** | -       | Snowflake external volume name      |
| `url`             | **string** | -       | Full JDBC URL including auth params |

```json
{
  "engines": {
    "snowflake": {
      "catalog-name": "my-glue-catalog",
      "external-volume": "my-external-volume",
      "url": "${SNOWFLAKE_JDBC_URL}"
    }
  }
}
```

* Put the credentials into the JDBC URL and read the whole URL from an environment variable, for example `"url": "${SNOWFLAKE_JDBC_URL}"`.

#### Usage Notes
- Requires all three configuration parameters
- Designed for large-scale analytical workloads in the cloud
- Integrates with AWS Glue for metadata management
- Uses external volumes for accessing Iceberg data
- Authentication parameters should be included in the JDBC URL
- For local development, consider using DuckDB as a substitute
- Provides enterprise features like data sharing and governance

### Spark SQL (`sparksql`)

Spark SQL generates Spark SQL table definitions and views for the Iceberg tables.

| Key             | Type       | Default   | Description                                                  |
|-----------------|------------|-----------|--------------------------------------------------------------|
| `view-catalog`  | **string** | -         | Catalog that contains generated views                        |
| `view-database` | **string** | `default` | Database that contains generated views when a catalog is set |

With both keys set, the generated view names are qualified as `catalog.database.view`. With only `view-database` set, they are qualified as `database.view` in the current catalog. With only `view-catalog` set, the views go into Spark SQL's `default` database.

```json
{
  "engines": {
    "sparksql": {
      "view-catalog": "analytics",
      "view-database": "reporting"
    }
  }
}
```

### Redshift (`redshift`)

Redshift generates Amazon Redshift table definitions and views for the Iceberg tables.

| Key             | Type       | Default  | Description                                                 |
|-----------------|------------|----------|-------------------------------------------------------------|
| `view-database` | **string** | -        | Database that contains generated views                      |
| `view-schema`   | **string** | `public` | Schema that contains generated views when a database is set |

With both keys set, the generated view names are qualified as `database.schema.view`. With only `view-schema` set, they are qualified as `schema.view` in the current database. With only `view-database` set, the views go into Redshift's `public` schema.

```json
{
  "engines": {
    "redshift": {
      "view-database": "analytics",
      "view-schema": "reporting"
    }
  }
}
```

### Trino (`trino`)

Trino generates Trino SQL definitions for Iceberg tables. Enable it by adding `"trino"` to `enabled-engines` alongside `"iceberg"`.

| Key            | Type       | Default  | Description                                                |
|----------------|------------|----------|------------------------------------------------------------|
| `view-catalog` | **string** | -        | Catalog that contains generated views                      |
| `view-schema`  | **string** | `public` | Schema that contains generated views when a catalog is set |

With both keys set, the generated view names are qualified as `catalog.schema.view`. With only `view-schema` set, they are qualified as `schema.view` in the current catalog. With only `view-catalog` set, the views go into Trino's `public` schema.

For a production deployment queried only through Trino, with no API:

```json
{
  "enabled-engines": ["flink", "iceberg", "trino"],
  "engines": {
    "trino": {
      "view-catalog": "analytics",
      "view-schema": "reporting"
    }
  }
}
```
