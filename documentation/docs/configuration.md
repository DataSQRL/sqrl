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
* **[Iceberg query engines](configuration-engine/iceberg-query)**: DuckDB, Snowflake, Spark SQL, and Redshift provide query access to Iceberg tables.
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

Optionally, it can also take a mutation database JSON generated during every compilation, and if it's kept and included in the config,
SQRL will check backward compatibility during compile making sure that mutation schemas will not get overwritten by mistake.

The `config` JSON object is passed to the Mustache templating engine to substitute template variable occurrences (e.g. `{{excludedTenant}}`) before the script is compiled.

```json
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
    }
  }
}
```

The example `script.config` above could be used to instantiate the following table definition in SQRL:
```sql
MyTable :=
    SELECT o.*
      FROM Orders AS o
      WHERE o.tenant_id <> {{excludedTenant}}
        {{#filters}}
        AND o.{{field}} IS {{^isNull}}NOT{{/isNull}} NULL
        {{/filters}}
      ORDER BY o.tenant_id DESC;
```

The final SQRL statement after the template variables got resolved will look like:
```sql
MyTable := 
    SELECT o.*
      FROM Orders AS o
      WHERE o.tenant_id <> 123
        AND o.total_amount IS NOT NULL
        AND o.coupon_code IS NULL
      ORDER BY o.tenant_id DESC;
```

### Include Other Sources

When multiple SQRL projects rely on the same logic or data catalog definitions, duplicating them would create unnecessary maintenance work.
Instead, include and reuse other SQRL projects with the `include` config. Each `include` key is the namespace used to import the included project's modules.
The required `package` value is the path to the included project's package JSON file, relative to the current project root.
DataSQRL uses the package file's parent directory as the include root and loads the SQRL files in that directory and its subdirectories.
Each `include` entry can define `config` overrides for the included project's `script.config` template values.

```json
{
  "script": {
    "main": "main.sqrl",                            // Main SQRL script for pipeline
    "include": {                                    // Include SQRL scripts from other projects
      "data_catalog": {                             // Namespace used in SQRL IMPORT statements
        "package": "../other-project/package.json", // Included project's package file
        "config": { ... }                           // Optional Mustache template overrides for the included script(s)
      }
    }
  }
}
```

:::note
Include namespaces support underscores. Prefer `_` as a separator, such as `data_catalog` instead of `data-catalog`, because dashes can have a different meaning in SQL.
:::

#### Include Example

Let's modify the `MyTable` example above to include the `Orders` table from a data catalog defined in another project.
First, define the package JSON accordingly:
```json
{
  "script": {
    "main": "my-project.sqrl",
    "config": {
      "excludedTenant": 123,
      "filters": [
        { "field": "total_amount", "isNull": false },
        { "field": "coupon_code", "isNull": true }
      ]
    },
    "include": {
      "data_catalog": {
        "package": "../other-project/other-project-shared-package.json"
      }
    }
  }
}
```

Then `my-project.sqrl` can import the data catalog sources:
```sql
IMPORT data_catalog.sources AS ctl;

MyTable :=
    SELECT o.*
      FROM ctl.Orders AS o
      WHERE o.tenant_id <> {{excludedTenant}}
        {{#filters}}
        AND o.{{field}} IS {{^isNull}}NOT{{/isNull}} NULL
        {{/filters}}
      ORDER BY o.tenant_id DESC;
```

:::important
When a SQRL script is in a subdirectory, e.g. `./my-module/module-script.sqrl`, use the `root` prefix to import an included namespace.
With the example above that would mean:
```sql
IMPORT root.data_catalog.sources;
```
:::

## Test-Runner (`test-runner`)

Configures how the DataSQRL test runner executes tests.
For streaming pipelines, use `required-checkpoints` to set a reliable time-interval for creating snapshots. Otherwise, configure a wall-clock delay via `delay-sec`.

```json
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
    "predicate-pushdown-rules": "LIMITED_RULES_NO_SOURCE", // configures the optimizer rules

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
      "default-limit": 10,         // default query result limit
      "paginated-results": false   // wrap generated query results in a page with pagination metadata
    }
  }
}
```

### Optimizer Configuration

Limit predicate pushdown to improve subgraph elimination and reduce the size of the physical computation graph by setting `predicate-pushdown-rules` to:
- `DEFAULT`: uses the default optimizer rules of the Flink engine
- `LIMITED_RULES_NO_SOURCE`: strips downstream predicate-pushdown related rules to maximize subgraph elimination
- `LIMITED_RULES`: additionally strips table source pushdown rules
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
- During `sqrl run` or `sqrl test`, DataSQRL first compiles the project with the same compile-time rules, then launches the generated artifacts, and resolves remaining `${VAR_NAME}` placeholders defined anywhere in the SQRL scripts from the runtime environment.
- If a required runtime placeholder is still unresolved when the generated artifact is launched, the run fails with an error identifying the missing variable.

Compile-time values are written into the generated artifacts under `build/` and `build/deploy/`. Use the secret placeholder syntax for values that must not be resolved or written during compile.
