# Default DataSQRL `package.json` Configuration

The following is the [default configuration file](https://raw.githubusercontent.com/DataSQRL/sqrl/refs/heads/main/sqrl-planner/src/main/resources/default-package.json) that user provided configuration files are merged on top of. It provides the default values for all configuration options.

```json
{
  "version": "1",
  "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
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
      "max-result-depth": 3,
      "default-limit": 10
    }
  },
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
  "connectors": {
    "kafka-mutation": {
      "connector": "kafka",
      "format": "flexible-json",
      "flexible-json.timestamp-format.standard": "ISO-8601",
      "properties.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
      "properties.group.id": "${KAFKA_GROUP_ID}",
      "properties.auto.offset.reset": "earliest",
      "topic": "${sqrl:table-name}"
    },
    "kafka": {
      "connector": "kafka",
      "format": "flexible-json",
      "flexible-json.timestamp-format.standard": "ISO-8601",
      "properties.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
      "properties.group.id": "${KAFKA_GROUP_ID}",
      "topic": "${sqrl:table-name}"
    },
    "iceberg": {
      "connector": "iceberg",
      "catalog-table": "${sqrl:table-name}",
      "warehouse": "iceberg-data",
      "catalog-type": "hadoop",
      "catalog-name": "mycatalog"
    },
    "postgres": {
      "connector": "jdbc-sqrl",
      "username": "${POSTGRES_USERNAME}",
      "password": "${POSTGRES_PASSWORD}",
      "url": "jdbc:postgresql://${POSTGRES_AUTHORITY}",
      "driver": "org.postgresql.Driver",
      "table-name": "${sqrl:table-name}"
    },
    "print": {
      "connector": "print",
      "print-identifier": "${sqrl:table-name}"
    }
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

## Connector Template Variables

The connector templates configured under `connectors` can use environment variables and SQRL-specific variables for dynamic configuration.

### Environment Variables

You can reference environment variables using the `${VAR_NAME}` placeholder syntax, for example `${POSTGRES_PASSWORD}`.
At runtime, these placeholders are automatically resolved using the environment variables defined in the system or deployment environment.

This can help decouple security credentials or add flexibility across different deployment environments.

### SQRL Variables

SQRL-specific variables start with a `sqrl:` prefix and are used for templating inside connector configuration options.
The proper syntax look like `${sqrl:<identifier>}`.

Supported identifiers include:
- `table-name`
- `original-table-name`
- `filename`
- `format`
- `kafka-key`

These are typically used within connector templates to inject table-specific or context-aware configuration values.

:::warning
Unresolved `${sqrl:*}` placeholders raise a validation error.
:::
