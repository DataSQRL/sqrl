# DataSQRL Configuration (`package.json` file)

DataSQRL projects are configured with one or more `*package.json` files which are merged in the order they are provided to the [DataSQRL command](https://docs.datasqrl.com/docs/main/compiler.md) – latter files override fields in earlier ones, objects are *deep-merged*, and array values are replaced wholesale. User provided configuration files are merged on top of the [default `package.json`](https://docs.datasqrl.com/docs/main/configuration-default.md). 

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
* **[flink](https://docs.datasqrl.com/docs/main/configuration-engine/flink.md)**: Apache Flink is a streaming and batch data processor
* **[postgres](https://docs.datasqrl.com/docs/main/configuration-engine/postgres.md)**: PostgreSQL is a realtime database
* **[kafka](https://docs.datasqrl.com/docs/main/configuration-engine/kafka.md)**: Apache Kafka is a streaming data platform (i.e. log engine)
* **[iceberg](https://docs.datasqrl.com/docs/main/configuration-engine/iceberg.md)**: Apache Iceberg is an analytic database format. Iceberg must be paired with a query engine for data access
* **[Iceberg query engines](https://docs.datasqrl.com/docs/main/configuration-engine/iceberg-query.md)**: DuckDB, Snowflake, Spark SQL, Redshift, and Trino provide query access to Iceberg tables.
* **[vertx](https://docs.datasqrl.com/docs/main/configuration-engine/vertx.md)**: Eclipse Vert.x is a reactive server framework

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

Configures the main SQRL script to compile, exposed APIs, and template values.

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

### API Versioning

The top-level `script.graphql` and `script.operations` fields configure a single API at version `v1`.
To serve multiple API versions concurrently, configure them under `script.api` instead.
API version names must start with `v` followed by a number, such as `v1` or `v2`. Each version must define a GraphQL `schema`, GraphQL `operations`, or an `openapi` compatibility specification. When a version does not define a schema, DataSQRL infers its GraphQL schema from the SQRL script.

DataSQRL serves each API version with its version as an endpoint prefix. With the default server configuration, the `v1` endpoints include `/v1/graphql`, `/v1/rest`, and `/v1/mcp`.

Use either the top-level fields or `script.api`. When `script.api` is configured, it defines the APIs that DataSQRL compiles and serves.

```json
{
  "script": {
    "main": "my-project.sqrl",
    "api": {
      "v1": {
        "schema": "api/schema.v1.graphqls",
        "operations": ["api/operations-v1/myop1.graphql"],
        "openapi": "api/openapi-v1.json"
      },
      "v2": {
        "schema": "api/schema.v2.graphqls",
        "operations": ["api/operations-v2/myop1.graphql"]
      }
    }
  }
}
```

DataSQRL generates an OpenAPI specification for every API version. Set the optional `openapi` field to a previously generated specification to check backward compatibility during compilation.
Compilation fails if the generated specification contains backward-incompatible changes.

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
        "config": { ... },                          // Optional Mustache template overrides for the included script(s)
        "standalone": true                          // Optional: the included project is deployed on its own (see Standalone Includes)
      }
    }
  }
}
```

> **Note**
>
> Include namespaces support underscores. Prefer `_` as a separator, such as `data_catalog` instead of `data-catalog`, because dashes can have a different meaning in SQL.

By default, the current project owns the internal tables of an included project like its own tables. See [Standalone Includes](#standalone-includes) for an included project that is deployed on its own.

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

> **Important**
>
> When a SQRL script is in a subdirectory, e.g. `./my-module/module-script.sqrl`, use the `root` prefix to import an included namespace.
> With the example above that would mean:
> ```sql
> IMPORT root.data_catalog.sources;
> ```

#### Standalone Includes

An included project is either a set of shared definitions or a project that is deployed on its own.
By default, the current project treats it as shared definitions: it owns the included project's internal tables like its own, so its deployment creates and configures their Kafka topics.

When the included project is deployed on its own, set `"standalone": true` on its include entry.
That project then owns its internal Kafka tables, also when the current project imports them or exports into them.

The deployment of the included project also writes what its own statements write, so the current project does not write it again.
Which statements those are follows from the `script.main` of the included project's package file, i.e. the `package` of the include entry: its deployment plans the main script and the scripts that the main script imports, transitively.

- **Subscriptions:** the deployment writes the topics of the subscriptions that it exposes, i.e. of the `SUBSCRIBE` tables, and of the tables that `SUBSCRIBE` functions read, in the main script and in the scripts that it imports inline (`IMPORT script.*`), transitively.
  The current project reads these topics for the subscriptions that it plans from these scripts, and for its own subscriptions on these tables, instead of computing the tables and writing them again.
  It writes the topics of the other subscriptions itself, e.g. of a script that the main script imports with a namespace (`IMPORT script`), since the deployment does not expose those.
- **Writes into shared sinks:** the deployment runs the `EXPORT` and `INSERT` statements of every script that it plans. The current project skips those that write into a sink that both deployments write: the log (`log.<name>`, and `logger.<name>` when the logger is `kafka`), the included project's internal Kafka tables, and external tables, i.e. tables with a connector.
  It reports a notice for each statement that it skips.

`kafka.json` lists the topics that the current project uses but does not write under `includedTopics` instead of `topics`, with only their names and `"include": "<namespace>"`, so the deployment leaves creating and configuring them, including their partition count, to the included project.

Without a `script.main` in the included project's package file, the current project cannot tell which statements the included project's deployment runs: it warns and runs them itself.
The topic of a subscription of the current project on a table of the included project, such as a `SUBSCRIBE` function on one of its internal tables, is named after that table. If the included project subscribes to the same table only in a script that the current project does not plan, both deployments write that topic.

Package files are merged, so the package file of a deployment can mark an include standalone that the base package file declares.
Every include entry requires `package`, so the deployment's entry repeats it.
For example, `reporting` includes `orders`, and in deployments `orders` is deployed on its own:

```json
// package.json
{
  "script": {
    "main": "reporting.sqrl",
    "include": {
      "orders": { "package": "../orders/package.json" }
    }
  }
}
```

```json
// package-deploy.json
{
  "script": {
    "include": {
      "orders": { "package": "../orders/package.json", "standalone": true }
    }
  }
}
```

Compiled with only `package.json`, e.g. for local tests, `reporting` owns the topics of the internal tables of `orders`. Compiled with both files, it leaves them to `orders`.

> **Important**
>
> The current project still reads and writes the topics of a standalone include through its own `kafka-mutation` and `kafka` [connector templates](#connector-templates-connectors), so it must resolve the same topic names and use the same key and value formats as the included project's deployment.
> A `topic` of the `kafka` template that uses `${sqrl:table-id}` differs between the projects, so the compilation fails when the current project reads a subscription topic of a standalone include.
> The included project's deployment must expose its subscriptions, i.e. run Kafka and the server, for the current project to read their topics.
> `standalone` does not cover internal Iceberg tables yet: the current project still owns those, and runs the included project's statements that write into them.
> A project whose Flink job would write no table, because standalone includes write all the tables that it subscribes to, fails to compile: subscribe through the API of the included project's deployment instead.

## Test-Runner (`test-runner`)

Configures how the DataSQRL test runner executes tests.
For streaming pipelines, set `delay-sec` to `-1` so the test runner waits until all Flink operators are idle and the `required-checkpoints` have completed before taking snapshots. Otherwise, `delay-sec` is a wall-clock upper bound on how long to wait for the job to finish.

```json
{
  "test-runner": {
    "snapshot-folder": "snapshots/myproject/", // Snapshots output directory (default: "./snapshots")
    "test-folder": "api/tests/",               // Directory containing test GraphQL queries (default: "./tests")
    "use-inferred-schema": true,               // Use inferred GraphQL schema when true, else use the one configured at "script.graphql" (default: true)
    "delay-sec": 30,                           // Max wait in sec for the job to finish before taking snapshots; ends early when the job terminates. -1 = wait until all operators are idle and required-checkpoints completed (streaming) or until the job completes (batch) (default: 30)
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
      "protocols": [               // public protocols exposed by the server
        "GRAPHQL",
        "REST",
        "MCP"
      ],
      "endpoints": "FULL",         // endpoint strategy: "FULL", "GRAPHQL", or "OPS_ONLY" (explicit operations only)
      "add-prefix": true,          // add an operation-type prefix to function names to ensure uniqueness
      "max-result-depth": 3,       // maximum depth of graph traversal when generating operations from a schema
      "default-limit": 10,         // default query result limit
      "paginated-results": false   // wrap generated query results in a page with pagination metadata
    },

    "tags": {                      // tag definitions by name; tags are validated and propagated when present (see Tags)
      "pii": { "values": ["masked", "raw"] }
    }
  }
}
```

`compiler.api.protocols` controls which public API routes are registered. The default enables `GRAPHQL`, `REST`, and `MCP`. Omitting `GRAPHQL` removes the versioned GraphQL HTTP, WebSocket, and GraphiQL routes while retaining the internal GraphQL execution engine used by REST and MCP. `REST` and `MCP` only register endpoints for operations that support those protocols.

`compiler.api.endpoints` controls operation generation and GraphQL flexibility. `FULL` generates operations from every query and mutation, `GRAPHQL` keeps the GraphQL endpoint flexible but exposes only explicitly defined REST and MCP operations, and `OPS_ONLY` accepts only explicitly defined named operations through a public GraphQL endpoint.

### Optimizer Configuration

`cost-model` controls how the optimizer assigns each table to an engine. The default is `DEFAULT`.
- `DEFAULT`: favors Flink, which computes the table at ingestion time, unless joins make Flink too expensive
- `WRITE`: always favors Flink
- `READ`: always favors the database, which computes the table at query time

The optimizer assigns each table to the cheapest engine. An `/*+ engine(...) */` hint limits the engines that the optimizer can pick. With `DEFAULT`, Flink and the database can have the same cost for a table. Then the optimizer assigns the table to Flink, so the table gets the same engine on every compile.

Limit predicate pushdown to improve subgraph elimination and reduce the size of the physical computation graph by setting `predicate-pushdown-rules` to:
- `DEFAULT`: uses the default optimizer rules of the Flink engine
- `LIMITED_RULES_NO_SOURCE`: strips downstream predicate-pushdown related rules to maximize subgraph elimination
- `LIMITED_RULES`: additionally strips table source pushdown rules
This setting only applies for Flink streaming when `compile-flink-plan` is enabled.

### Tags (`compiler.tags`)

Tags such as `@pii` mark tables in their doc strings. When `compiler.tags` is configured, the compiler validates every tag against these definitions, propagates tags from the tables a query reads to the table it defines, and checks that each table in the API declares the tags it inherits. When `compiler.tags` is absent, tags are not analyzed and doc strings are plain text.

The compiler only checks that tags are defined, valid, and propagated correctly. It does not judge whether a tag or a declassification is appropriate.

```json
{
  "compiler": {
    "tags": {
      "classification": { "values": ["unclassified", "secret", "top_secret"] },
      "pii": { "values": ["masked", "raw", "sensitive"] },
      "gdpr": {},
      "jurisdiction": { "required-columns": { "jurisdiction": "STRING" } }
    }
  }
}
```

Each key is a tag name, and its definition has these optional fields:

| Field              | Meaning |
|--------------------|---------|
| `values`           | The allowed values, ordered from **lowest to highest**. |
| `required-columns` | The columns, by name, with their type, that every table carrying the tag must have. |

Tag names and values may only contain letters, digits, underscores and dashes, and are case-insensitive. Values and required column names must be unique within a tag, also ignoring case, and each required column type must be a valid column type. When several package files are combined, their tag definitions are merged per tag.

There are two kinds of tags:
- **Boolean tag**: has no `values`, e.g. `"gdpr": {}`. Its only value is `true`.
- **Ordered tag**: has `values`. When two values meet, the higher one wins: `top_secret` is higher than `secret`.

#### Declaring tags in doc strings

Write a tag as `@name` or `@name=value` in the doc string of a table, preceded by whitespace or at the start of the doc string, so `user@example.com` is not a tag. `@name` means `@name=true`, which is only valid for boolean tags; ordered tags need a value. A tag that is not defined, or has a value that is not allowed, is a compile error.

```sql
/** Customer profiles @pii=raw @classification=secret
 * # Columns:
 * - email: The contact email @gdpr
 */
CustomerProfiles := SELECT id, email, state FROM Customers;
```

A table's declared tags are all tags in its doc string, including column documentation, and in the comments of its columns in `CREATE TABLE` (e.g. `email STRING COMMENT 'Contact email @gdpr'`). A `CREATE TABLE ... LIKE` also gets the tags in the comments of the columns it copies. Tags count toward the whole table, also when they are written on a column. If a tag appears more than once, the highest value counts. Tags stay part of the doc string, so they also appear in the generated API documentation.

#### Propagation and validation

A table **inherits** the tags of every table its definition reads, including tables that are only joined, filtered on, or referenced in a subquery. When several inputs carry the same tag, the highest value is inherited. Tables created with `CREATE TABLE` or imported from a source inherit nothing.

A table's **effective** tags are its inherited tags combined with its declared tags, taking the highest value per tag. A doc string can raise a tag's value but not lower it. The effective tags are what downstream tables inherit, also through hidden tables.

The compiler checks:
- Every table and function in the API declares each tag it inherits with the same or a higher value. A table without a doc string of its own that returns the same columns as the table it selects from (e.g. `SELECT * FROM Customers WHERE ...`), or a `DISTINCT` table (e.g. `Customers := DISTINCT CustomersStream ON id ORDER BY updated_at DESC`), takes the doc string of the table it selects from, and with it its tags. A [column-addition statement](https://docs.datasqrl.com/docs/main/sqrl-language.md#column-addition-statement) keeps the doc string of the table definition it extends.
- These tables need not declare their inherited tags, but still carry them: hidden tables, relationships (e.g. `Customers.orders := ...`), which return rows of their target table, [test tables](https://docs.datasqrl.com/docs/main/sqrl-language.md#testing), which are never deployed, and views defined with Flink SQL `CREATE VIEW`, which have no doc string.
- A table that `INSERT INTO` or `EXPORT` writes into declares the tags of the written data, if other tables read it or the pipeline manages it, i.e. an internal table defined with an `engine` hint, such as an Iceberg table defined in a separate script. Its readers and its deployment only have its declared tags. Writes into an external table that nothing reads are not checked, since the data leaves the pipeline.
- Every table, hidden or public, whose effective tags include a tag with `required-columns` has those columns, with a type that is accepted wherever a function parameter of the required type is expected.

The compiler reports all violations in one compile, with the table, the tag, the required and found values, and the input tables the tag comes from.

#### Declassification

The `declassify` hint lists tag names, without the `@`, that a table may drop or lower:

```sql
/*+declassify(pii, gdpr, classification) */
/** Customers per state @classification=unclassified */
CustomerCountsByState := SELECT state, COUNT(*) AS num_customers FROM CustomerProfiles GROUP BY state;
```

The table does not inherit the listed tags, so its value for each of them is the value its doc string declares, or no value if it declares none. Downstream tables inherit that value. In the example, `CustomerCountsByState` drops `pii` and `gdpr` and lowers `classification` to `unclassified`. Listing a tag that is not defined is a compile error; listing a tag the table does not inherit is a warning. `declassify` also works on hidden tables.

#### Tags in the build output

The compiler writes the effective tags of every table and view, hidden or public, to the build output:
- `build/pipeline_explain.txt` lists them under `Tags:` for each node, and `build/pipeline_explain.json` and the pipeline visualization under `tags`.
- The database deployment plan (e.g. `build/deploy/plan/postgres.json`) lists them under `tags` for each table and view, mapped from tag name to value (`true` for boolean tags).
- The Kafka deployment plan (`build/deploy/plan/kafka.json`) lists them the same way for each topic that stores a table, including the topics of mutation tables. Topics of the test runner and of standalone includes have no tags.

A table defined with `CREATE TABLE` has the tags that its statement declares, also when it is an internal table that an `EXPORT` writes into.

For a table that must declare its inherited tags and passes validation, the effective tags are exactly its declared tags.

## Connector Templates (`connectors`)

Connector templates are used to configure how the engines in the pipeline connect to each other for data exchange. The connector templates use Flink SQL connector configuration options which are mapped to the configuration for each engine.

The [default connector configuration](https://docs.datasqrl.com/docs/main/configuration-default.md) works for most local use cases without adjustments.
Refer to the individual engine configuration for connector configuration options related to that engine.

## Environment Variables (`${VAR}`)

Environment variables can be referenced with two placeholder types:

- `${VAR_NAME}` references a non-secret environment variable, for example `${POSTGRES_HOST}`. DataSQRL treats this syntax as non-secret, even if the variable name contains words like `PASSWORD` or `TOKEN`.
- `${{VAR_NAME}}` references a secret environment variable, for example `${{POSTGRES_PASSWORD}}`. Secret placeholders are not resolved during compile and are converted to `${VAR_NAME}` in generated artifacts so the actual value is resolved only when the pipeline runs.

DataSQRL resolves environment variables at two different times:

- During `sqrl compile`, DataSQRL resolves available, non-secret environment variables in user-provided `package.json` [connector configuration](https://docs.datasqrl.com/docs/main/configuration-default.md) and in Flink `CREATE TABLE ... WITH (...)` options in SQRL scripts.
- If a `${VAR_NAME}` placeholder is not available during `sqrl compile`, it is left unchanged in the generated build artifacts so it can still be supplied later.
- During `sqrl run` or `sqrl test`, DataSQRL first compiles the project with the same compile-time rules, then launches the generated artifacts, and resolves remaining `${VAR_NAME}` placeholders defined anywhere in the SQRL scripts from the runtime environment.
- If a required runtime placeholder is still unresolved when the generated artifact is launched, the run fails with an error identifying the missing variable.

Compile-time values are written into the generated artifacts under `build/` and `build/deploy/`. Use the secret placeholder syntax for values that must not be resolved or written during compile.
