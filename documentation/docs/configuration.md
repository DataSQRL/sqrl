# DataSQRL Configuration (`package.json` file)

DataSQRL projects are configured with one or more `*package.json` files which are merged in the order they are provided to the [DataSQRL command](compiler) – latter files override fields in earlier ones, objects are *deep-merged*, and array values are replaced wholesale. User provided configuration files are merged on top of the [default `package.json`](configuration-default). 

The `version` field specifies the version of the configuration file which is currently `1`.

---

## Engines (`enabled-engines`)

The engines that the pipeline compiles to.

```json
{
  "enabled-engines": ["flink", "postgres", "kafka", "vertx"]
}
```

DataSQRL supports the following engines:
* **[flink](configuration-engine/flink)**: Apache Flink is a streaming and batch data processor
* **[postgres](configuration-engine/postgres)**: PostgreSQL is a realtime database
* **[kafka](configuration-engine/kafka)**: Apache Kafka is a streaming data platform (i.e. log engine)
* **[iceberg](configuration-engine/iceberg)**: Apache Iceberg is an analytic database format. Iceberg must be paired with a query engine for data access
* **[duckdb](configuration-engine/duckdb)**: DuckDB is a vectorized database query engine that can read Iceberg tables.
* **[snowflake](configuration-engine/snowflake)**: Snowflake is an analytic database query engine that can read Iceberg tables.
* **[vertx](configuration-engine/vertx)**: Eclipse Vert.x is a reactive server framework

Guidelines for choosing the enabled engines in a pipeline:
* Always choose one data processor (i.e. "flink")
* Choose a log engine (i.e. "kafka") to produce data streams
* Choose a database engine (realtime or analytic) to produce data that can be queried
* Choose a server engine (i.e. "vertx") to produce data APIs (e.g. GraphQL, REST, MCP)
* Choose a log engine (i.e. "kafka") to support data ingestion or subscriptions in the API
* If picking an analytic table format as the database, also choose one or more compatible query engines.
* Choose at most one log or server engine, but choosing multiple database engines is supported.
* When choosing a query engine that operates in the cloud (e.g. snowflake), substitute for a locally executable query engine (i.e. "duckdb") for testing and running the pipeline locally.

The individual engines are configured under the **`engines`** field. The following example configures a Flink-specific setting:

```json
{
  "engines": {
    "flink": {
      "config": {
        "table.exec.source.idle-timeout": "10s"
      }
    }
  }
}
```

Refer to the engine configuration documentation for more information on how to configure individual engines.

## Source Files (`script`)

Configures the main SQRL script to compile, the (optional) GraphQL schema for the exposed API, and (optional) list of operations defined as GraphQL queries.

Shared SQRL scripts can be configured under `shared` to define reusable packages, such as common data catalogs, that are imported by multiple SQRL projects.
Each shared script package is structured like a regular SQRL project, but its root directory must contain a `package.json` configuration file, which may be minimal and can provide metadata or default `script.config` values for template variables used by the shared SQRL files.
Each `shared` entry uses its key as the import namespace, points `path` to the shared package root, and can define per-project `config` overrides for the shared package defaults.

Optionally it can also take a mutation database JSON that is generated during every compilation, and if it's kept and included in the config,
SQRL will check backward compatibility during compile making sure that mutation schemas will not get overwritten by mistake.

The `config` JSON object is passed to the Mustache templating engine to substitute template variable occurrences (e.g. `{{table}}`) before the script is compiled.

```json5
{
  "script": {
    "main": "my-project.sqrl",                         // Main SQRL script for pipeline
    "graphql": "api/schema.v1.graphqls",               // GraphQL schema defines the API
    "operations": ["api/operations-v1/myop1.graphql"], // List of GraphQL queries that define operations which are exposed as API endpoints
    "database": "my-mutation-database.json",           // Check backward compatibility for mutation schema during compilation
    "config": {                                        // Arbitrary JSON object used by the mustache templating engine to instantiate SQRL files
      "excludedTenant": 123,
      "filters": [
        { "field": "total_amount", "isNull": false },
        { "field": "coupon_code", "isNull": true }
      ]
    },
    "shared": {                                        // Shared SQRL script(s) that can be imported in the main SQRL script
      "data-catalog": {
        "path": "../shared-project",                   // Relative path to the shared project root
        "config": { ... }                              // Optional mustache template overrides
      }
    }
  }
}
```

The example `script.config` above could be used to instantiate the following table definition in SQRL:
```sql
IMPORT `data-catalog`.sources;

MyTable := SELECT
             o.*
           FROM sources.Orders AS o
           WHERE o.tenant_id <> {{excludedTenant}}
            {{#filters}}
             AND o.{{field}} IS {{^quoted}}NOT{{/quoted}} NULL
            {{/filters}}
            ORDER BY o.tenant_id DESC;
```

## Test-Runner (`test-runner`)

Configures how the DataSQRL test runner executes tests.
For streaming pipelines, use `required-checkpoints` to set a reliable time-interval for creating snapshots. Otherwise, configure a wall-clock delay via `delay-sec`.

```json5
{
  "test-runner": {
    "snapshot-folder": "snapshots/myproject/", // Snapshots output directory (default: "./snapshots")
    "test-folder": "api/tests/",               // Directory containing test GraphQL queries (default: "./tests")
    "use-inferred-schema": true,               // Use inferred GraphQL schema when true, else use the one configured at "script.graphql" (default: true)
    "delay-sec": 30,                           // Wait between data-load and taking snapshot in sec. Set -1 to disable (default: 30)
    "mutation-delay-sec": 0,                   // Pause(s) between mutation queries (default: 0)
    "required-checkpoints": 0,                 // Minimum completed Flink checkpoints before taking snapshots (requires delay-sec = -1)
    "create-topics": ["topic1", "topic2"],     // Kafka topics to create before tests start
    "headers": {                               // Any HTTP headers to add during the test execution. For example, JWT auth header
      "Authorization": "Bearer token"
    }
  }
}
```

## Compiler (`compiler`)

Configuration options that control the compiler, such as where logging output is produced, how the pipeline plan is written out, what cost model to use determine data processing step to engine allocation, and what protocols are exposed in the API.

```json
{
  "compiler": {
    "logger": "print",             // "print" | "none"
    "extended-scalar-types": true, // support extended scalar types in generated GraphQL
    "compile-flink-plan": true,    // produce a Flink physical plans (not supported in batch)
    "cost-model": "DEFAULT",       // cost model to use for DAG optimization ("DEFAULT" | "READ" | "WRITE")
    "predicate-pushdown-rules": "LIMITED_TABLE_SOURCE_RULES", // configures the optimizer rules

    "explain": {                   // controls what and how the compiler writes pipeline plans to build/pipeline_*
      "sql":      false,          // include SQL code in the plan
      "logical":  false,           // include the logical plan for each table
      "physical": false,          // include the physical plan for each table
      "sorted":   true           // ensure deterministic ordering (mostly for tests)
    },

    "api": {
      "protocols": [               // protocols that are being exposed by the server
        "GRAPHQL",
        "REST",
        "MCP"
      ],
      "endpoints": "FULL",         // endpoint generation strategy ("FULL", "GRAPHQL", "OPS_ONLY")
      "add-prefix": true,          // add an operation-type prefix to function names to ensure uniqueness
      "max-result-depth": 3,       // maximum depth of graph traversal when generating operations from a schema
      "default-limit": 10          // default query result limit
    }
  }
}
```

### Optimizer Configuration

Limit predicate pushdown to improve subgraph identification and reduce the size of the physical computation graph by setting `predicate-pushdown-rules` to:
- `DEFAULT`: uses the default optimizer rules of the Flink engine
- `LIMITED_TABLE_SOURCE_RULES`: strip all table source related predicate pushdown rules
- `LIMITED_RULES`: additionally strips additional filter rules
This setting only applies for Flink streaming when `compile-flink-plan` is enabled.

## Connector Templates (`connectors`)

Connector templates are used to configure how the engines in the pipeline connect to each other for data exchange. The connector templates use Flink SQL connector configuration options which are mapped to the configuration for each engine.

The [default connector configuration](configuration-default) works for most local use cases without adjustments.
Refer to the individual engine configuration for connector configuration options related to that engine.

## Environment Variables (`${VAR}`)

Environment variables can be referenced with two placeholder types:

- `${VAR_NAME}` references a non-secret environment variable, for example `${POSTGRES_HOST}`. DataSQRL treats this syntax as non-secret, even if the variable name contains words like `PASSWORD` or `TOKEN`.
- `${{VAR_NAME}}` references a secret environment variable, for example `${{POSTGRES_PASSWORD}}`. Secret placeholders are not resolved during compile and are converted to `${VAR_NAME}` in generated artifacts so the actual value is resolved only when the pipeline runs.

DataSQRL resolves environment variables at two different times:

- During `sqrl compile`, DataSQRL resolves available, non-secret environment variables in user-provided `package.json` [connector configuration](configuration-default) and in Flink `CREATE TABLE ... WITH (...)` options in SQRL scripts.
- If a `${VAR_NAME}` placeholder is not available during `sqrl compile`, it is left unchanged in the generated build artifacts so it can still be supplied later.
- During `sqrl run` or `sqrl test`, DataSQRL first compiles the project with the same compile-time rules, then launches the generated artifacts and resolves remaining `${VAR_NAME}` placeholders defined anywhere in the SQRL scripts from the runtime environment.
- If a required runtime placeholder is still unresolved when the generated artifact is launched, the run fails with an error identifying the missing variable.

Compile-time values are written into the generated artifacts under `build/` and `build/deploy/`. Use the secret placeholder syntax for values that must not be resolved or written during compile.
