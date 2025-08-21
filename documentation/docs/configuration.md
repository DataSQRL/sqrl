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

| Key          | Type       | Default   | Notes                                                                                              |
|--------------|------------|-----------|----------------------------------------------------------------------------------------------------|
| `config`     | **object** | see below | Copied verbatim into the generated Flink SQL job (e.g. `"table.exec.source.idle-timeout": "5 s"`). |

```json
{
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
    }
  }
}
```

> **Built-in connector templates**  
> `postgres`, `postgres_log-source`, `postgres_log-sink`,  
> `kafka`, `kafka-keyed`, `kafka-upsert`,  
> `iceberg`, `print`.

### Kafka (`kafka`)

The default configuration only declares the engine; topic definitions are injected at **plan** time.  
Additional keys (e.g. `bootstrap.servers`) may be added under `config`.

### Vert.x (`vertx`)

A GraphQL server that routes queries to the backing database/log engines.  
No mandatory keys; connection pools are generated from the overall plan.
In terms of security, we support JWT auth, that can be specified under the `config` section.

| Key          | Type       | Default   | Notes                     |
|--------------|------------|-----------|---------------------------|
| `config`     | **object** | see below | Vert.x JWT configuration. |

```json5
{
  "engines": {
    "vertx" : {
      "authKind": "JWT",
      "config": {
        "jwtAuth": {
          "pubSecKeys": [
            {
              "algorithm": "HS256",
              "buffer": "<signer-secret>"   // Base64 encoded signer secret string
            }
          ],
          "jwtOptions": {
            "issuer": "<jwt-issuer>",
            "audience": ["<jwt-audience>"],
            "expiresInSeconds": "3600",
            "leeway": "60"
          }
        }
      }
    }
  }
}
```

### Postgres (`postgres`)

No mandatory keys. Physical DDL (tables, indexes, views) is produced automatically.

### Iceberg (`iceberg`)

Used as a *table-format* engine together with a query engine such as Flink, Snowflake, or DuckDB.

### DuckDB (`duckdb`)

| Key   | Type       | Default          | Description    |
|-------|------------|------------------|----------------|
| `url` | **string** | `"jdbc:duckdb:"` | Full JDBC URL. |

### Snowflake (`snowflake`)

| Key               | Type       | Default | Description                          |
|-------------------|------------|---------|--------------------------------------|
| `catalog-name`    | **string** | –       | Glue catalog.                        |
| `external-volume` | **string** | –       | Snowflake external volume name.      |
| `url`             | **string** | –       | Full JDBC URL including auth params. |

---

## Connectors (`connectors`)

```json5
{
  "connectors": {
    "postgres": { "connector": "jdbc-sqrl",  /*...*/ },
    "kafka-mutation": { "connector" : "kafka", /*...*/ },
    "kafka": { "connector" : "kafka", /*...*/ },
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
    },

    "api": {
      "protocols": [               // protocols that are being exposed by the server
        "GRAPHQL",
        "REST",
        "MCP"
      ],
      "endpoints": "FULL",         // endpoint generation strategy (FULL, GRAPHQL, OPS_ONLY)
      "add-prefix": true,          // add an operation-type prefix before function names
      "max-result-depth": 3        // maximum depth of graph traversal when generating operations from a schema
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

| Key          | Type         | Description                                                   |
|--------------|--------------|---------------------------------------------------------------|
| `main`       | **string**   | Path to the main `.sqrl` file.                                |
| `graphql`    | **string**   | Optional GraphQL schema file (defaults to `schema.graphqls`). |
| `operations` | **string[]** | Optional GraphQL operation definitions.                       |

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

| Key                    | Type         | Default       | Meaning                                                                                |
|------------------------|--------------|---------------|----------------------------------------------------------------------------------------|
| `snapshot-folder`      | **string**   | `./snapshots` | Snapshots output directory.                                                            |
| `test-folder`          | **string**   | `./tests`     | Tests output directory.                                                                |
| `delay-sec`            | **number**   | `30`          | Wait between data-load and snapshot. Set `-1` to disable.                              |
| `mutation-delay-sec`   | **number**   | `0`           | Pause(s) between mutation queries.                                                     |
| `required-checkpoints` | **number**   | `0`           | Minimum completed Flink checkpoints before assertions run (requires `delay-sec = -1`). |
| `create-topics`        | **string[]** | -             | Kafka topics to create before tests start.                                             |
| `headers`              | **object**   | -             | Any HTTP headers to add during the test execution. For example, JWT auth header.       |

---

## Templating & Variable Resolution

The DataSQRL launcher supports dynamic resolution of variable placeholders at runtime.

* **Environment variables**: use `${VAR_NAME}` as a placeholder. Example: `${POSTGRES_PASSWORD}`.
* **SQRL variables** use `${sqrl:<identifier>}` and are filled automatically by the compiler, mostly inside connector templates.  
  Common identifiers include `table-name`, `original-table-name`, `filename`, `format`, and `kafka-key`.

:::warning
Unresolved `${sqrl:*}` placeholders raise a validation error.
:::

---

## Internal Environment Variables

For engines that may be running as standalone services inside the DataSQRL Docker container,
we use the following environment variables internally:

* **Kafka**
  * `KAFKA_BOOTSTRAP_SERVERS`
  * `KAFKA_GROUP_ID`
* **PostgreSQL**
  * `POSTGRES_VERSION`
  * `POSTGRES_HOST`
  * `POSTGRES_PORT`
  * `POSTGRES_DATABASE`
  * `POSTGRES_AUTHORITY`
  * `POSTGRES_JDBC_URL`
  * `POSTGRES_USERNAME`
  * `POSTGRES_PASSWORD`

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
    "duckdb": {
      "url": "jdbc:duckdb:"
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
    },
    "api": {
      "protocols": ["GRAPHQL", "REST", "MCP"],
      "endpoints": "FULL",
      "add-prefix": true,
      "max-result-depth": 3
    }
  },
  "connectors": {
    "postgres": { "connector": "jdbc-sqrl",  /*...*/ },
    "kafka-mutation": { "connector" : "kafka", /*...*/ },
    "kafka": { "connector" : "kafka", /*...*/ },
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
