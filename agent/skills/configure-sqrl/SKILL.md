---
name: configure-sqrl
description: Use when configuring a DataSQRL project's `*-package.json` files. Use for engine selection (Flink, Postgres, Kafka, Iceberg and its query engines DuckDB, Snowflake, Spark SQL, Redshift, Trino), API protocols, compiler options, runtime settings, or deployment/infrastructure settings.
---
Update the configuration of a DataSQRL project by editing its `*-package.json` files or creating new ones. Read **Configuration Files** first (how the files are split and layered), then look up the key you need under **Configuration Content**.

The configuration supports environment placeholders in the configuration fields that process them. Read **Environment Variables** before adding credentials or tokens: the normal and secret forms have different compile-time behavior, and support is not universal across every configuration field.

# Configuration Files

## Base Config and Environment Overlays

For projects that need multiple deployment profiles, split configuration into a **base** (shared) and thin **per-environment overlays** to reduce duplication. This is a recommended project convention: SQRL accepts any ordered list of package JSON files and has no special meaning for the filenames or environment names.

- **`<project>-shared-package.json`** is the base config file that includes common settings to every environment which could be `version`, `enabled-engines`, `script.main`, `script.api`, and the `engines`/`compiler` settings.
- **`<project>-<env>-package.json`** (one per environment `-test`, `-prod`, `-local`, `-dev`) — add or overwrite ONLY what differs from the base configuration file (e.g. `script.config.environment`, `test-runner`, an engine override). Note that `-dev` is a local development environment, like `-local`.

Example for the base config and environment overlays:
The `lending360` project with a test and a production environment:

**`lending360-shared-package.json`** — the base: everything both environments share.

```json
{
  "version": "1",
  "enabled-engines": ["flink", "postgres", "kafka", "vertx"],
  "script": {
    "main": "lending360.sqrl",
    "api": {
      "v1": {
        "schema": "lending360-api/schema.v1.graphqls",
        "operations": ["lending360-api/operations.v1.graphql"]
      }
    }
  },
  "engines": {
    "flink": {
      "config": {
        "execution.runtime-mode": "STREAMING",
        "table.exec.source.idle-timeout": "30 s"
      }
    }
  },
  "compiler": {
    "api": { "protocols": ["GRAPHQL", "REST", "MCP"], "endpoints": "FULL" }
  }
}
```

**`lending360-test-package.json`** the test overlay: ONLY what test changes.

```json
{
  "script": {
    "config": { "environment": "test" }
  },
  "engines": {
    "flink": {
      "config": { "table.exec.source.idle-timeout": "1 s" } // Set to 1 s not to wait during test
    }
  },
  "test-runner": {
    "delay-sec": -1,
    "required-checkpoints": 1,
    "snapshot-folder": "snapshots/lending360/",
    "test-folder": "lending360-api/tests"
  }
}
```

**`lending360-prod-package.json`** the production overlay.

```json
{
  "script": {
    "config": { "environment": "prod" }
  },
  "engines": {
    "flink": {
      "deployment": {
        "jobmanager-size": "small",
        "taskmanager-size": "large.mem",
        "taskmanager-count": 3
      }
    }
  }
}
```

Rules for writing the configuration files:

* **Merging is per field, last file wins.** In the example above, the test overlay's `table.exec.source.idle-timeout: "1 s"` replaces the base's `"30 s"`, while `execution.runtime-mode: "STREAMING"` from the base is kept. An overlay does not replace the whole `engines.flink.config` object.
* **`test-runner` belongs only in the `-test` overlay**.
* **Every independently compiled or tested project and sub-project MUST have a named package file for each environment it uses:** `<project-or-sub-project>-<env>-package.json` (for example, `fraud_store-test-package.json`). Do not use an unqualified standalone package file for a deployable sub-project. The name lets the agent's package-discovery fallback identify the package that belongs to each build.
* If settings are common across environments or sibling sub-projects, share them through a `*-shared-package.json` base; keep only environment- or sub-project-specific settings in the named package files.
* **Fix duplicated configuration:** move common settings to `<project>-shared-package.json` and retain only differing fields in `<project>-<env>-package.json`.

## Sub-projects (multiple deployments in one project)

One project folder may hold several **sub-projects**, meaning separate deployments over the same source files, each with its own configuration.

Every sub-project has **its own per-environment overlays** and layers them under a base. The base is either **its own** or **shared with sibling sub-projects**. Never combine sub-project specific configurations from different sub-projects.

| File | Role |
|------|------|
| `<sub-project>-shared-package.json` | own base — `enabled-engines`, `script.main`, `script.api`, settings common to all of this sub-project's environments. Use one base per sub-project when the settings the sub-projects share differ (e.g. different engine sets). |
| `<project>-shared-package.json` | shared base — only what is common to **every** sub-project layering under it (`version`, common `engines`/`compiler` settings, `enabled-engines` if identical). Whatever differs — at least `script.main` — is declared in the sub-project's own files, so the base stays valid for each of them. |
| `<sub-project>-test-package.json` | test overlay — `test-runner` with its **own** `snapshot-folder`, plus test-only overwrites |
| `<sub-project>-prod-package.json` | production overlay — production `script.config`, API protocols, deployment sizing and prod-only overwrites |

An available examples directory may include a useful multi-deployment reference. For example, `healthcare-study/` in `datasqrl-examples` illustrates separate stream, API, and analytics deployments alongside a shared data catalog; use it only when it is present and relevant.

```
fraud_store-shared-package.json      fraud_training-shared-package.json
fraud_store-test-package.json        fraud_training-test-package.json
fraud_store-prod-package.json
```

Example with **one shared base** for two sub-projects that differ only in their main script and snapshot folder:

```
fraud-shared-package.json            # version, enabled-engines, common engines/compiler settings — no script.main
fraud_store-test-package.json        # "script": { "main": "fraud_store.sqrl" }, test-runner with snapshots/fraud_store/
fraud_store-prod-package.json        # "script": { "main": "fraud_store.sqrl" }, prod settings
fraud_training-test-package.json     # "script": { "main": "fraud_training.sqrl" }, test-runner with snapshots/fraud_training/
fraud_training-prod-package.json     # "script": { "main": "fraud_training.sqrl" }, prod settings
```

```bash
/opt/agent/cmd.sh test -r . fraud-shared-package.json fraud_store-test-package.json -b fraud_store
/opt/agent/cmd.sh test -r . fraud-shared-package.json fraud_training-test-package.json -b fraud_training
```

Each sub-project's `-test-package.json` is its own test run. To setup a test and run, invoke the `/test-sqrl` skill.

## How to Compile/Test

**Compile**

Compile by layering base then overlay (merged last-wins):

```bash
/opt/agent/cmd.sh compile -r . lending360-shared-package.json lending360-test-package.json -b lending360
```

The `-b <sub-project>` flag gives each sub-project its own build folder *inside* `build/`, so a compile keeps sub-project's build artifacts intact.

**Test**
Invoke the `/test-sqrl` skill for which test packages to run and how.

# Configuration Content

## Engines (`enabled-engines`, `engines`)

**Enable the minimal set of engines the project actually uses — nothing more.** Enabled engines determine what pipeline and deployment artifacts SQRL produces; some engines also provision or run services. Start from `flink` and add an engine only when a concrete capability below requires it.
At the end of the implementation, **always** check your engine set whether if is minimal or not, and prune engines that are no longer used.

```json
{
  "enabled-engines": ["flink", "postgres", "vertx"]
}
```

Enable each engine **only if** its trigger is present:

| Engine                                 | Enable ONLY IF                                                                                             | Omit when                                                                   |
|----------------------------------------|------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| **flink**                              | always — streaming/batch data processor                                                                    | never omit                                                                  |
| **postgres**                           | the API serves data from database-backed tables (query/point-lookup endpoints, full-text or vector search) | there is no served DB query — e.g. the only output is a data-lake or EXPORT |
| **kafka**                              | there is a `SUBSCRIBE` subscription or a CREATE TABLE annotated with `/*+engine(kafka)*/` hint             | the API is plain request/response with no subscriptions or mutations        |
| **iceberg**                            | a table is written to a data lake — must be paired with at least one Iceberg query engine (the two rows below) | there is no data-lake output                                                |
| **duckdb**                             | iceberg is enabled and the project serves an API, runs tests or runs locally over the Iceberg tables — the only *full* query engine, the one the server queries | iceberg is not used — never enable duckdb on its own                        |
| **snowflake, sparksql, redshift, trino**      | iceberg is enabled and the project needs to query Iceberg tables through that external engine( shallow query engines): the compiler only generates that engine's table definitions and views | iceberg is not used, or the project does not use that external engine |
| **vertx**                              | the project exposes a GraphQL/REST/MCP API                                                                 | it is a pure ETL/export pipeline with no served API                         |

Common minimal recipes (pick the smallest that fits, then adjust):
* **API over a database, no streaming pub/sub** → `["flink", "postgres", "vertx"]`
* **API with subscriptions or mutations** → `["flink", "postgres", "kafka", "vertx"]`
* **Data lake queried from Snowflake, no API** → base `["flink", "iceberg", "duckdb"]` for tests and local runs, prod overlay `["flink", "iceberg", "snowflake"]`
* **API over a data lake** → `["flink", "iceberg", "duckdb", "vertx"]`
* **API over a data lake that Redshift also queries** → `["flink", "iceberg", "duckdb", "redshift", "vertx"]`
* **Catalog / schema test** → `["flink", "postgres", "vertx"]`

Only enable iceberg plus query engines and postgres if both high-volume & high latency and low latency querying is needed in the same project.

Iceberg query engines come in two kinds. `duckdb` is the full engine: it is connected to the server and runs the API queries and the tests. `snowflake`, `sparksql`, `redshift` and `trino` are shallow engines: the compiler generates table definitions and query SQL to run separately in those external engines, and they are not integrated with the DataSQRL server. Read [iceberg-query.md](iceberg-query.md) before choosing between them.

### Configuring the selected engines

Enabled engines are configured under the `engines` field. Engine settings that hold for every environment go in the base `<project>-shared-package.json`; only the ones that differ go in an environment overlay, checkout [Base Config and Environment Overlays](#base-config-and-environment-overlays).

**Before writing the configuration for an engine, ALWAYS read its reference file below.** Option names, nesting and value formats are engine-specific and are not guessable. Read the file for **every** engine in your set first, then write the `engines` block. 

| Engine in your set | Read this before configuring it |
|--------------------|---------------------------------|
| `flink` | [flink.md](flink.md) |
| `vertx` | [vertx.md](vertx.md) |
| `kafka` | [kafka.md](kafka.md) |
| `postgres` | [postgres.md](postgres.md) |
| `iceberg` | [iceberg.md](iceberg.md) |
| `duckdb`, `snowflake`, `sparksql`, `redshift`, `trino` | [iceberg-query.md](iceberg-query.md) |

Important rules:
* An enabled engine with **no** entry under `engines` runs on its defaults. Add a setting only when a requirement, a connector, or the engine's reference file calls for it.
* Keep `engines` entries aligned with `enabled-engines`. SQRL removes a configuration for a disabled engine and warns when that configuration came from a user package; delete it yourself when pruning the engine.

## Script (`script`)

```json
{
  "script": {
    "main": "my-project.sqrl", // Main SQRL script for pipeline
    "api": {
      "v1": { //default API version
        "schema": "fraud_store-api/schema.v1.graphqls", // GraphQL schema defines the API
        "operations": ["fraud_store-api/operations.v1.graphql"], //List of GraphQL queries that define operations which are exposed as API endpoints
        "openapi": "fraud_store-api/openapi.v1.json" //Optional OpenAPI baseline. See the /from-api skill
      }
    },
    "config": { //Arbitrary JSON object used by the mustache templating engine to instantiate SQRL file templating variables
      "environment": "kafka" 
    }
  }
}
```

### API Versioning and Compatibility

`script.api` serves one or more versioned APIs. Version keys must be `v` followed by a number (`v1`, `v2`); each version requires a `schema`, `operations`, or an `openapi` compatibility specification. SQRL infers the schema from the SQRL script when one is not configured, and generates an OpenAPI specification for every version. Set `openapi` to a previously generated specification when compilation must reject backward-incompatible API changes.

For one `v1` API only, the legacy top-level fields are also supported:

```json
{
  "script": {
    "main": "my-project.sqrl",
    "graphql": "api/schema.v1.graphqls",
    "operations": ["api/operations.v1.graphql"],
    "database": "my-mutation-database.json"
  }
}
```

Use **either** top-level `graphql`/`operations` or `script.api`, never both. `script.database` is an optional mutation-database JSON emitted by a previous compile; keeping it in the config makes SQRL check mutation-schema backward compatibility.

### Included Projects (`script.include`)

Reuse scripts from **another SQRL project** (e.g. a shared data catalog) by declaring it under `script.include`. 
Each key under `script.include` is the **namespace** the consumer imports it under; `package` is the path, **relative to the current project root**, to **any package JSON file at the included project's top level** (`package.json`, `<name>-shared-package.json`, …). DataSQRL takes that file's parent directory as the include root and walks it and all its subfolders, loading every `.sqrl` script under the namespace.

```json
"script": {
  "main": "consumer.sqrl",
  "include": {
    "<namespace>": { "package": "../other-project/package.json", "config": { } }
  }
}
```

Example:
```json
{
  "script": {
    "main": "consumer.sqrl",
    "include": {
      "data_catalog": {                                                   // namespace → IMPORT data_catalog.<script>
        "package": "../other-project/other-project-shared-package.json",  // path; relative from this project's root to the other project's TOP-LEVEL package file (its <name>-shared-package.json, or package.json); that file's folder + all subfolders is the include root
        "config": { "environment": "prod" }                               // optional mustache overrides for that project's {{...}} template vars
      }
    }
  }
}
```

* The **key** (`<namespace>`) is the import namespace. Name it with **underscores** (`data_catalog`), because a dash has a different meaning in SQL. `package` is the path **relative to the current project** to the other project's top-level package JSON file — its parent directory is the include root, and the SQRL files in that directory and its subdirectories are loaded; `config` supplies Mustache override values for that project's `script.config` template variables (`{{...}}`).
- **Template values for the included scripts.** An included project's scripts may use `{{...}}` placeholders — variables such as `{{environment}}`, sections such as `{{#is_batch}}…{{/is_batch}}`. DataSQRL renders them from two sources: the `script.config` of the package file named in `package`, and the include entry's own `config`, which overrides it. Add `config` only when you have values to pass: it must hold at least one property, so an empty `{}` is rejected (`must have at least 1 properties at location [/script/include/<namespace>/config]`).
- **Declare `script.include` in the config that declares `script.main`** — the base config (or, with a base shared by several sub-projects, each sub-project's file that declares its `script.main`). Overlays that only add engine or test-runner settings need no `script.include`.
- **Declare only the projects this project actually needs**, not everything the repository happens to contain: every declared project is copied into `build/` at compile time, so an unnecessary declaration are expensive.
- At compile time the included project is **copied into `build/<namespace>/`** from which IMPORT statements are resolved. Invoke the `/implement-sqrl` skill (SQRL Language Spec → IMPORT Statement → Included projects / multi-project).

## Test Runner (`test-runner`)

Put test-runner settings in a test overlay when using base/overlay packages. For streaming pipelines, prefer `required-checkpoints` with `delay-sec: -1`; otherwise use a wall-clock `delay-sec`.

```json
{
  "test-runner": {
    "snapshot-folder": "snapshots/myproject/",
    "test-folder": "api/tests/",
    "use-inferred-schema": true,
    "delay-sec": -1,
    "mutation-delay-sec": 0,
    "required-checkpoints": 1,
    "create-topics": ["input-topic"],
    "headers": { "Authorization": "Bearer ${TEST_TOKEN}" }
  }
}
```

Defaults are `./snapshots`, `./tests`, `true`, `30`, `0`, and `0` respectively for the first six fields. `create-topics` and `headers` are optional, non-empty values.

## Connector Templates (`connectors`)

The `connectors` field holds the templates that decide how the engines in the pipeline exchange table data with each other: the Kafka topic for a mutation table, the Postgres table behind a materialized query, the Iceberg table for a lake sink, the log/print sink.
**No `connectors` field result in default usage.** Add or overwrite a template only when a requirement forces it: a fixed topic naming scheme, a non-default format, extra Kafka client properties, or credentials that do not come from the standard environment variables. **Before any change, ALWAYS read [connector-templates.md](connector-templates.md) first.** It lists every default template in full and the fields that can be overwritten. An overwritten template replaces only the fields you name.

Note that these connector templates apply to table definition managed by DataSQRL are different from the source/sink connectors explicitly defined in SQRL (`CREATE TABLE ... WITH ('connector' = ...)`). For those, invoke the `/manage-connector` skill.

## Compiler (`compiler`)

```json
{
  "compiler": {
    "logger": "print",
    "compile-flink-plan": true,
    "extended-scalar-types": true,
    "cost-model": "DEFAULT",
    "predicate-pushdown-rules": "LIMITED_RULES_NO_SOURCE",
    "explain": {
      "sql": false,
      "logical": false,
      "physical": false,
      "sorted": true
    },
    "api": {
      "protocols": ["GRAPHQL", "REST", "MCP"],
      "endpoints": "FULL",
      "add-prefix": true,
      "max-result-depth": 3,
      "default-limit": 10,
      "paginated-results": false
    }
  }
}
```

`logger` is `print` or `none`. `extended-scalar-types` enables extended generated GraphQL scalars. `compile-flink-plan` controls Flink physical-plan generation and is not supported for batch pipelines. `cost-model` is `DEFAULT`, `READ`, or `WRITE`.

`explain` controls plan output under `build/`: use `sql`, `logical`, or `physical` to include each representation, and `sorted` for deterministic order. Although the current schema accepts `text` and `visual`, the current compiler configuration interface does not consume them; do not rely on those keys.

### Optimizer configuration

`predicate-pushdown-rules` applies only to Flink streaming when `compile-flink-plan` is enabled:

* `DEFAULT` uses the normal Flink optimizer rules.
* `LIMITED_RULES_NO_SOURCE` strips downstream predicate-pushdown rules to maximize subgraph elimination.
* `LIMITED_RULES` additionally strips table-source pushdown rules.
* `LIMITED_TABLE_SOURCE_RULES` is also accepted by the configuration schema; use it only when a specific SQRL version/workload requires it, as it's deprecated.

### API protocol configuration

DataSQRL's authoritative API model is GraphQL; REST and MCP are generated from it. `protocols` may contain `GRAPHQL`, `REST`, and/or `MCP` and defaults to all three. `endpoints` is one of:

* `FULL`: generated REST/MCP operations for schema queries and mutations, plus explicit operations.
* `GRAPHQL`: flexible GraphQL but only explicit operations for other protocols.
* `OPS_ONLY`: explicit GraphQL operations only.

`add-prefix` prefixes generated operation names to avoid collisions. `max-result-depth` controls generated REST/MCP result traversal depth, `default-limit` is the generated query limit, and `paginated-results: true` wraps multi-row query results in a page with pagination metadata.

## Environment Variables (`${VAR}`)

`${NAME}` is a normal environment-variable template; `${NAME:-fallback}` and `${NAME:=fallback}` both use `fallback` when `NAME` is absent. In supported compile-time fields, a present value is resolved and can be written into `build/` or `build/deploy/`; otherwise the normal template remains for runtime resolution. Do not use this form for a credential that must stay out of compile artifacts.

`${{NAME}}` is a secret template and accepts only a variable name. Even if `NAME` is available at compile time, SQRL does not read it: in supported paths it rewrites `${{NAME}}` to `${NAME}` in the generated artifact for runtime resolution. This conversion applies only to user `connectors` configuration and Flink `CREATE TABLE ... WITH (...)` properties—not arbitrary JSON, including `engines.vertx.config`.
