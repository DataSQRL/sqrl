# DataSQRL Configuration (`package.json` file)

DataSQRL projects are configured with one or more **JSON** files.  
Unless a file is passed explicitly to `datasqrl compile -c …`, the compiler looks for a `package.json` in the working directory; if none is found the **built-in default** (shown [here](#default-configuration)) is applied.

Multiple files can be provided; they are merged **in order** – later files override earlier ones, objects are *deep-merged*, and array values are replaced wholesale.

---

## Top-Level Keys

| Key               | Type         | Default                                  | Purpose                                                        |
|-------------------|--------------|------------------------------------------|----------------------------------------------------------------|
| `version`         | **number**   | `1`                                      | Configuration schema version – must be `1`.                    |
| `enabled-engines` | **string[]** | `["vertx","postgres","kafka","flink"]`   | Ordered list of engines that form the runtime pipeline.        |
| `engines`         | **object**   | –                                        | Per-engine configuration (see below).                          |
| `compiler`        | **object**   | [see defaults](#compiler-compiler)       | Controls compilation, logging, and generated artefacts.        |
| `dependencies`    | **object**   | `{}`                                     | Aliases for packages that can be `IMPORT`-ed from SQRL.        |
| `discovery`       | **object**   | `{}`                                     | Rules for automatic table discovery when importing data files. |
| `script`          | **object**   | –                                        | Points to the main SQRL script and GraphQL schema.             |
| `package`         | **object**   | –                                        | Optional metadata (name, description, etc.) for publishing.    |
| `test-runner`     | **object**   | [see defaults](#test-runner-test-runner) | Integration test execution settings.                           |

---

## 1. Engines (`engines`)

Each sub-key below `engines` must match one of the IDs in **`enabled-engines`**.

```
{
  "engines": {
    "<engine-id>": {
      "type": "<engine-id>",        // optional; inferred from key if omitted
      "config": { … },              // engine-specific knobs (Flink SQL options, etc.)
      "connectors": { … }           // templates for table sources & sinks
    }
  }
}
```

### Flink (`flink`)
| Key | Type | Default | Notes |
|-----|------|---------|-------|
| `mode` | `"streaming"` \| `"batch"` | `"streaming"` | Execution mode used during plan compilation **and** job submission. |
| `config` | object | `{}` | Copied verbatim into the generated Flink SQL job (e.g. `"table.exec.source.idle-timeout": "5 s"`). |
| `connectors` | object | *see default list* | Connector templates (JDBC, Kafka, files, Iceberg…). Field values support variable interpolation (below). |

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
| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `schema-type` | `"aws-glue"` | – | External catalog implementation. |
| `catalog-name` | string | – | Glue catalog. |
| `external-volume` | string | – | Snowflake external volume name. |
| `url` | string | – | Full JDBC URL including auth params. |

---

## Compiler (`compiler`)

```jsonc
{
  "compiler": {
    "logger": "print",            // "print" | any configured log engine | "none"
    "extendedScalarTypes": true,  // expose extended scalar types in generated GraphQL
    "compile-flink-plan": true,   // compile Flink physical plans where supported

    "explain": {                  // artifacts in build/pipeline_*.*
      "visual":   true,
      "text":     true,
      "sql":      false,
      "logical":  true,
      "physical": false,
      "sorted":   true            // deterministic ordering (mostly for tests)
    }
  }
}
```

---

## 3. Dependencies (`dependencies`)

```json
"dependencies": {
  "myalias": {
    "name": "folder-name",
    "version": "1",       // version identifier
    "variant": "test"     // identifier for the source variant
  }
}
```

If only `name` is given the key acts as a **local folder alias**.

---

## Discovery (`discovery`)

| Key | Type | Default | Purpose |
|-----|------|---------|---------|
| `pattern` | **string (regex)** | `null` | Filters which external tables are automatically exposed in `IMPORT …` statements. Example: `"^public\\..*"` |

---

## Script (`script`)

| Key | Type | Description |
|-----|------|-------------|
| `main` | **string** | Path to the main `.sqrl` file. |
| `graphql` | **string** | Optional GraphQL schema file (defaults to `schema.graphqls`). |

---

## Package Metadata (`package`)

| Key | Required | Description |
|-----|----------|-------------|
| `name` | **yes** | Reverse-DNS style identifier (`org.project.module`). |
| `description` | no | Short summary. |
| `license` | no | SPDX license id or free-text. |
| `homepage` | no | Web site. |
| `documentation` | no | Docs link. |
| `topics` | no | String array of tags/keywords. |

---

## Test-Runner (`test-runner`)

| Key                    | Type         | Default       | Meaning                                                                                |
|------------------------|--------------|---------------|----------------------------------------------------------------------------------------|
| `snapshot-dir`         | **string**   | `./snapshots` | Snapshots output directory.                                                            |
| `test-dir`             | **string**   | `./tests`     | Tests output directory.                                                                |
| `delay-sec`            | **number**   | `30`          | Wait between data-load and snapshot. Set `-1` to disable.                              |
| `mutation-delay-sec`   | **number**   | `0`           | Pause(s) between mutation queries.                                                     |
| `required-checkpoints` | **number**   | `0`           | Minimum completed Flink checkpoints before assertions run (requires `delay-sec = -1`). |
| `create-topics`        | **string[]** | -             | Kafka topics to create before tests start.                                             |

---

## Template & Environment Variables

* **Environment variables** use `${VAR_NAME}` — resolved by the DataSQRL launcher at runtime.  
  Example: `${JDBC_PASSWORD}`.
* **SQRL variables** use `${sqrl:<identifier>}` and are filled automatically by the compiler, mostly inside connector templates.  
  Common identifiers include `table-name`, `original-table-name`, `filename`, `format`, and `kafka-key`.

Unresolved `${sqrl:*}` placeholders raise a validation error.

---

## Default Configuration

The built-in fallback (excerpt - full version [here](https://github.com/DataSQRL/sqrl/blob/main/sqrl-tools/sqrl-config/src/main/resources/default-package.json)):

```jsonc
{
  "version": 1,
  "enabled-engines": ["vertx","postgres","kafka","flink"],
  "engines": {
    "flink": {
      "connectors": {
        "postgres": { "connector": "jdbc-sqrl", … },
        "kafka":   { "connector": "kafka", … },
        "kafka-keyed": { … },
        "kafka-upsert": { … },
        "localfile": { … },
        "iceberg": { … },
        "print": { "connector": "print" }
      }
    },
    "snowflake": {
      "schema-type": "aws-glue",
      "catalog-name": "${SNOWFLAKE_CATALOG_NAME}",
      "external-volume": "${SNOWFLAKE_EXTERNAL_VOLUME}",
      "url": "jdbc:snowflake://${SNOWFLAKE_ID}.snowflakecomputing.com/?…"
    }
  },
  "test-runner": {
    "delay-sec": 30,
    "mutation-delay-sec": 1,
    "headers": {
      "Authorization": "Bearer token123",
      "Content-Type": "application/json"
    }
  }
}
```
