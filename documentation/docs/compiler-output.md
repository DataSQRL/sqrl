# Compiler Output

The [`compile`](compiler#compile-command) command writes two kinds of output. Compilation outputs in the `build` folder describe the data model and the pipeline the compiler planned. Deployment outputs in `build/deploy/plan` are the artifacts each engine executes. The [`run`](compiler#run-command) and [`test`](compiler#test-command) commands produce the same output before they execute the pipeline.

The output is the basis for validating a pipeline before you deploy it, whether you or a coding agent wrote the SQRL script. Each file shows the pipeline at a different level of detail, from the data model down to the physical plan of each engine. This page lists the files and what they contain at a high level. The best way to learn the details is to compile a project and inspect the files:

```bash
docker run --rm -v $PWD:/workspace datasqrl/cmd compile my-project-prod-package.json
```

:::info
The output goes to `build` and `build/deploy` by default. Use the `--build` and `--target` options of the [compiler](compiler) to write it elsewhere, for example to keep the output of several sub-projects apart.
:::

## Levels of Detail

The output files form a hierarchy. Start at the top to check that the pipeline does the right thing, then go down to check that it does it the right way.

| Level           | Question it answers                                                       | Files                                                                                     |
|-----------------|---------------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| Source          | What exactly did the compiler compile?                                    | `pipeline_source.sqrl`, `package.json`                                                    |
| Data model      | Which entities, fields, and relationships does the API expose?            | `data_model_visual.html`, `inferred_schema.graphqls`                                      |
| Logical DAG     | How does data flow between tables, and which engine computes each table?  | `pipeline_explain.txt`, `pipeline_explain.json`, `pipeline_visual.html`                   |
| Physical plan   | How does each engine execute its part of the pipeline?                    | `flink-explained-plan.txt`, `flink-compiled-plan.json`, `postgres-schema.sql`, `vertx.json`, ... |

## Compilation Outputs

The `build` folder contains the following files that describe the compiled pipeline:

| File                              | Contents                                                                                                                                                                                                                                             |
|-----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `pipeline_explain.txt`            | A compact text description of every table in the pipeline DAG: its type (`stream` or `state`), the stage (engine) that computes it, primary key, timestamp, estimated row count, schema with data types and nullability, input tables, and annotations such as sort orders. |
| `pipeline_explain.json`           | The complete pipeline DAG as JSON. In addition to the information in `pipeline_explain.txt`, it includes source imports with their connector configuration, API queries with their parameters, the SQL and logical plan of each node, and the documentation from the SQRL script. |
| `pipeline_visual.html`            | An interactive visualization of the pipeline DAG. Open it in a browser and click a node to inspect its schema, SQL, logical plan, and physical plan.                                                                                                |
| `pipeline_source.sqrl`            | The complete SQRL source the compiler processed, with all imported scripts and connector definitions inlined and template variables substituted.                                                                                                     |
| `pipeline_mutation_database.json` | The schemas of the tables written by API mutations, including engine, DDL, columns, and keys. Keep this file and reference it in the [`script.database`](configuration#source-files-script) configuration to have the compiler check backward compatibility of mutation schemas. |
| `inferred_schema.graphqls`        | The GraphQL schema the compiler generated for the API (only for projects with an API).                                                                                                                                                              |
| `data_model_visual.html`          | An interactive diagram of the API data model for the latest API version, showing the types, their fields, and the relationships between them (only for projects with an API).                                                                       |

The `build` folder also contains the staged project files the compiler read: the SQRL scripts, the GraphQL files, and a `package.json` with the effective configuration after merging all package files with the defaults. Check `package.json` to verify which configuration was applied. The compiler log is in the `logs` folder.

### Validating with Compilation Outputs

- **`data_model_visual.html`** and **`inferred_schema.graphqls`** show the data model at the highest level. Use them to confirm that the API exposes the expected entities with the expected fields, and that relationships connect the right entities.
- **`pipeline_explain.txt`** shows the logical plan one table at a time. Use it to confirm that each table is computed by the intended engine, that keys, timestamps, and nullability match your expectations, and that the lineage through the input tables is correct. Its compact format makes it the preferred input for coding agents, and it is easy to diff between compilations.
- **`pipeline_visual.html`** shows the same DAG as a graph for human review. Follow the data flow from sources to API endpoints and sinks, and drill into individual nodes for the SQL and plans.
- **`pipeline_explain.json`** contains the most detail in a machine-readable format. Use it to build automated checks, such as policies that every source has a watermark, that tables with sensitive columns are not exposed, or that no table runs on an unexpected engine.
- **`pipeline_source.sqrl`** shows exactly what was compiled. Use it to verify that the right connector definitions were imported for the environment and that template variables resolved as intended.

To include SQL, logical plans, or physical plans in `pipeline_explain.txt`, enable them in the [compiler `explain` configuration](configuration#compiler-compiler).

## Deployment Outputs

The `build/deploy/plan` folder contains the deployment artifacts for each enabled engine. The files depend on the engines in your [configuration](configuration). The DataSQRL [run command](compiler#run-command) and the [Flink SQL Runner](https://github.com/DataSQRL/flink-sql-runner) deploy the pipeline from these files. See [Deployment](deployment) for how to deploy them to production.

### Flink

| File                               | Contents                                                                                                                                                       |
|------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `flink-sql.sql`                    | The complete Flink SQL script: function registrations, `CREATE TABLE` statements for sources and sinks, views for intermediate tables, and the statement set with all `INSERT INTO` statements. |
| `flink-sql-no-functions.sql`       | The same script without the function registrations.                                                                                                            |
| `flink-functions.sql`              | The function registrations for system library and user-defined functions.                                                                                      |
| `flink-compiled-plan.json`         | The Flink compiled plan that is executed. It fixes every operator, its state, and its connector configuration.                                                 |
| `flink-compiled-plan-summary.json` | A condensed version of the compiled plan that lists each operator with a short description.                                                                    |
| `flink-explained-plan.txt`         | Flink's optimized execution plan as an operator tree for each sink: sources, watermark assigners, calculations, exchanges, joins, aggregations, ranks, and sinks. |
| `flink-config.yaml`                | The Flink configuration for the job, such as runtime mode, checkpointing, and state backend.                                                                   |
| `flink.json`                       | A manifest of the Flink job with the SQL statements and the connectors, formats, and functions it depends on.                                                  |

The `build/deploy/flink` folder contains the data files that sources read locally, mapped to `${DATA_PATH}`.

### Kafka

| File         | Contents                                                                                               |
|--------------|--------------------------------------------------------------------------------------------------------|
| `kafka.json` | The topics the pipeline creates, with name, partitions, replication factor, and configuration, plus the topics the test runner uses. |

### PostgreSQL

| File                  | Contents                                                                                                    |
|-----------------------|-------------------------------------------------------------------------------------------------------------|
| `postgres-schema.sql` | `CREATE TABLE` statements for the tables Flink writes to, and `CREATE INDEX` statements for the selected indexes. |
| `postgres-views.sql`  | Views for tables that PostgreSQL computes at query time.                                                    |
| `postgres.json`       | All statements in execution order, including required extensions.                                          |

### Iceberg

| File                                                | Contents                                                                         |
|-----------------------------------------------------|----------------------------------------------------------------------------------|
| `iceberg-schema.sql`, `iceberg-views.sql`           | Table and view definitions for the Iceberg tables.                               |
| `iceberg-duckdb-schema.sql`, `iceberg-duckdb-views.sql` | Table and view definitions for the query engine that reads the Iceberg tables, such as DuckDB. |
| `iceberg.json`                                      | The plans for the Iceberg tables and their query engines.                       |

### Vert.x Server

| File                         | Contents                                                                                                                                       |
|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `vertx.json`                 | The server model for each API version: the GraphQL schema and, for every query, mutation, and subscription, the parameterized database query or topic it executes. |
| `vertx-config.json`          | The server configuration: ports, database connection, CORS, and protocol settings.                                                             |
| `vertx-<version>-openapi.json` | The OpenAPI specification of the REST API for the API version.                                                                               |

### Tests

| File        | Contents                                                                                                       |
|-------------|----------------------------------------------------------------------------------------------------------------|
| `test.json` | The test plan: database views for tables annotated with `/*+ test */`, and the test queries, mutations, and subscriptions. |

### Validating with Deployment Outputs

The deployment outputs show the physical plan, the most detailed level. Use them to validate how the pipeline executes and whether it will hold up in production:

- **`flink-explained-plan.txt`** shows how Flink executes each table. Check that deduplications use the expected rank strategy, that joins are temporal or interval joins where expected rather than regular joins with unbounded state, and that watermarks are assigned with the intended delay.
- **`flink-compiled-plan.json`** is the exact plan that runs. Compare it between versions to detect changes that are incompatible with existing savepoints.
- **`postgres-schema.sql`** and **`postgres-views.sql`** show which tables are materialized, which are computed at query time, and which indexes the compiler selected. Check that the API queries in `vertx.json` are served by an index.
- **`vertx.json`** and the OpenAPI specification show exactly which SQL each API endpoint executes. Use them to review the cost of queries and to verify that filters and access restrictions are applied.
- **`flink-config.yaml`**, **`kafka.json`**, and **`vertx-config.json`** show the operational settings. Validate them against your deployment requirements, such as checkpoint intervals, topic retention, and partitioning.

Together, these files let you build an ensemble of reviews: humans inspect the visualizations, coding agents read the text plans, and automated checks enforce compliance, governance, and reliability requirements on the JSON outputs.
