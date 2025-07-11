# DataSQRL Configuration (`package.json` file)

DataSQRL projects are configured with one or more **JSON** files.  
Unless a file is passed explicitly to `datasqrl compile -c ...`, the compiler looks for a `package.json`
in the working directory; if none is found the **built-in default** (shown [here](#default-configuration)) is applied.

Multiple files can be provided; they are merged **in order** – latter files override earlier ones, objects are *deep-merged*, and array values are replaced wholesale.

---

## Top-Level Keys

| Key               | Type         | Default                                  | Purpose                                                        |
|-------------------|--------------|------------------------------------------|----------------------------------------------------------------|
| `version`         | **number**   | `1`                                      | Configuration schema version – must be `1`.                    |
| `enabled-engines` | **string[]** | `["vertx","postgres","kafka","flink"]`   | Ordered list of engines that form the runtime pipeline.        |
| `engines`         | **object**   | –                                        | Engine specific configuration (see below).                     |
| `connectors`      | **object**   | [see defaults](#connectors-connectors)   | External system connectors configuration (see below).          |
| `compiler`        | **object**   | [see defaults](#compiler-compiler)       | Controls compilation, logging, and generated artifacts.        |
| `dependencies`    | **object**   | `{}`                                     | Aliases for packages that can be `IMPORT`-ed from SQRL.        |
| `discovery`       | **object**   | `{}`                                     | Rules for automatic table discovery when importing data files. |
| `script`          | **object**   | –                                        | Points to the main SQRL script and GraphQL schema.             |
| `package`         | **object**   | –                                        | Optional metadata (name, description, etc.) for publishing.    |
| `test-runner`     | **object**   | [see defaults](#test-runner-test-runner) | Integration test execution settings (see below).               |

---

## Engines (`engines`)

Each sub-key below `engines` must match one of the IDs in **`enabled-engines`**.

```json5
{
  "engines": {
    "<engine-id>": {
      "type": "<engine-id>", // optional; inferred from key if omitted
      "config": { /*...*/ }  // engine-specific knobs (Flink SQL options, etc.)
    }
  }
}
```

### Flink (`flink`)

| Key          | Type   | Default   | Notes                                                                                              |
|--------------|--------|-----------|----------------------------------------------------------------------------------------------------|
| `config`     | object | see below | Copied verbatim into the generated Flink SQL job (e.g. `"table.exec.source.idle-timeout": "5 s"`). |

```json
{
  "config": {
    "execution.runtime-mode": "STREAMING",
    "execution.target": "local",
    "execution.attached": true,
    "rest.address": "localhost",
    "rest.port": 8081,
    "state.backend.type": "rocksdb",
    "table.exec.resource.default-parallelism": 1,
    "taskmanager.memory.network.max": "800m"
  }
}
```

> **Built-in connector templates**  
> `postgres`, `postgres_log-source`, `postgres_log-sink`,  
> `kafka`, `kafka-keyed`, `kafka-upsert`,  
> `iceberg`, `localfile`, `print`.

### Kafka (`kafka`)

The default configuration only declares the engine; topic definitions are injected at **plan** time.  
Additional keys (e.g. `bootstrap.servers`) may be added under `config`.

### Vert.x (`vertx`)

A GraphQL server that routes queries to the backing database/log engines.  
No mandatory keys; connection pools are generated from the overall plan.

### Postgres (`postgres`)

No mandatory keys. Physical DDL (tables, indexes, views) is produced automatically.

### Iceberg (`iceberg`)

Used as a *table-format* engine together with a query engine such as Flink or Snowflake.

### Snowflake (`snowflake`)

| Key               | Type         | Default | Description                          |
|-------------------|--------------|---------|--------------------------------------|
| `schema-type`     | `"aws-glue"` | –       | External catalog implementation.     |
| `catalog-name`    | string       | –       | Glue catalog.                        |
| `external-volume` | string       | –       | Snowflake external volume name.      |
| `url`             | string       | –       | Full JDBC URL including auth params. |

---

## Connectors (`connectors`)

```json5
{
  "connectors": {
    "postgres": { "connector": "jdbc-sqrl",  /*...*/ },
    "kafka-mutation": { "connector" : "kafka", /*...*/ },
    "kafka": { "connector" : "kafka", /*...*/ },
    "localfile": { "connector": "filesystem", /*...*/ },
    "iceberg": { "connector": "iceberg", /*...*/ },
    "postgres_log-source": { "connector": "postgres-cdc", /*...*/ },
    "postgres_log-sink": { "connector": "jdbc-sqrl", /*...*/ },
    "print": { "connector": "print", /*...*/ }
  }
}
```

---

## Compiler (`compiler`)

```json5
{
  "compiler": {
    "logger": "print",             // "print" | any configured log engine | "none"
    "extended-scalar-types": true, // expose extended scalar types in generated GraphQL
    "compile-flink-plan": true,    // compile Flink physical plans where supported
    "cost-model": "DEFAULT",       // cost model to use for DAG optimization

    "explain": {                   // artifacts in build/pipeline_*.*
      "text":     true,
      "sql":      false,
      "logical":  true,
      "physical": false,
      "sorted":   true,            // deterministic ordering (mostly for tests)
      "visual":   true
    }
  }
}
```

---

## Dependencies (`dependencies`)

```json
{
  "dependencies": {
    "my-alias": {
      "folder": "folder-name"
    }
  }
}
```

If only `folder` is given the dependency key (`my-alias` in the above example) acts as a **local folder alias**.

---

## Discovery (`discovery`)

| Key       | Type               | Default | Purpose                                                                                                     |
|-----------|--------------------|---------|-------------------------------------------------------------------------------------------------------------|
| `pattern` | **string (regex)** | `null`  | Filters which external tables are automatically exposed in `IMPORT …` statements. Example: `"^public\\..*"` |

---

## Script (`script`)

| Key       | Type       | Description                                                   |
|-----------|------------|---------------------------------------------------------------|
| `main`    | **string** | Path to the main `.sqrl` file.                                |
| `graphql` | **string** | Optional GraphQL schema file (defaults to `schema.graphqls`). |

---

## Package Metadata (`package`)

| Key             | Required | Description                                          |
|-----------------|----------|------------------------------------------------------|
| `name`          | **yes**  | Reverse-DNS style identifier (`org.project.module`). |
| `description`   | no       | Short summary.                                       |
| `license`       | no       | SPDX license id or free-text.                        |
| `homepage`      | no       | Web site.                                            |
| `documentation` | no       | Docs link.                                           |
| `topics`        | no       | String array of tags/keywords.                       |

---

## Test-Runner (`test-runner`)

| Key                     | Type         | Default       | Meaning                                                                                |
|-------------------------|--------------|---------------|----------------------------------------------------------------------------------------|
| `snapshot-folder`       | **string**   | `./snapshots` | Snapshots output directory.                                                            |
| `test-folder`           | **string**   | `./tests`     | Tests output directory.                                                                |
| `delay-sec`             | **number**   | `30`          | Wait between data-load and snapshot. Set `-1` to disable.                              |
| `mutation-delay-sec`    | **number**   | `0`           | Pause(s) between mutation queries.                                                     |
| `required-checkpoints`  | **number**   | `0`           | Minimum completed Flink checkpoints before assertions run (requires `delay-sec = -1`). |
| `create-topics`         | **string[]** | -             | Kafka topics to create before tests start.                                             |

---

## Template & Environment Variables

* **Environment variables** use `${VAR_NAME}` — resolved by the DataSQRL launcher at runtime.  
  Example: `${JDBC_PASSWORD}`.
* **SQRL variables** use `${sqrl:<identifier>}` and are filled automatically by the compiler, mostly inside connector templates.  
  Common identifiers include `table-name`, `original-table-name`, `filename`, `format`, and `kafka-key`.

Unresolved `${sqrl:*}` placeholders raise a validation error.

---

## Default Configuration

The built-in fallback (excerpt - full version [here](https://raw.githubusercontent.com/DataSQRL/sqrl/refs/heads/main/sqrl-planner/src/main/resources/default-package.json)):

```json5
{
  "version": 1,
  "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
  "engines": {
    "flink": {
      "config": {
        "execution.runtime-mode": "STREAMING",
        "execution.target": "local",
        "execution.attached": true,
        "rest.address": "localhost",
        "rest.port": 8081,
        "state.backend.type": "rocksdb",
        "table.exec.resource.default-parallelism": 1,
        "taskmanager.memory.network.max": "800m"
      }
    },
    "snowflake": {
      "schema-type": "aws-glue",
      "catalog-name": "${SNOWFLAKE_CATALOG_NAME}",
      "external-volume": "${SNOWFLAKE_EXTERNAL_VOLUME}",
      "url": "jdbc:snowflake://${SNOWFLAKE_ID}.snowflakecomputing.com/?..."
    }
  },
  "compiler": {
    "logger": "print",
    "extended-scalar-types": true,
    "compile-flink-plan": true,
    "cost-model": "DEFAULT",
    "explain": {
      "text": true,
      "sql": false,
      "logical": true,
      "physical": false,
      "sorted": true,
      "visual": true
    }
  },
  "connectors": {
    "postgres": { "connector": "jdbc-sqrl",  /*...*/ },
    "kafka-mutation": { "connector" : "kafka", /*...*/ },
    "kafka": { "connector" : "kafka", /*...*/ },
    "localfile": { "connector": "filesystem", /*...*/ },
    "iceberg": { "connector": "iceberg", /*...*/ },
    "postgres_log-source": { "connector": "postgres-cdc", /*...*/ },
    "postgres_log-sink": { "connector": "jdbc-sqrl", /*...*/ },
    "print": { "connector": "print", /*...*/ }
  },
  "test-runner": {
    "snapshot-folder": "./snapshots",
    "test-folder": "./tests",
    "delay-sec": 30,
    "mutation-delay-sec": 0,
    "required-checkpoints": 0
  }
}
```
