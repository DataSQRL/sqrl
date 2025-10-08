# Flink Engine Configuration

Apache Flink is a streaming and batch data processor that serves as the core data processing engine in DataSQRL pipelines.

## Configuration Options

| Key          | Type       | Default   | Notes                                                                                              |
|--------------|------------|-----------|----------------------------------------------------------------------------------------------------| 
| `config`     | **object** | see below | Copied verbatim into the generated Flink SQL job (e.g. `"table.exec.source.idle-timeout": "5 s"`). |

## Example Configuration

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

## Built-in Connector Templates

The following connector templates are available when using Flink:
- `postgres` - JDBC connector for PostgreSQL
- `postgres_log-source` - PostgreSQL CDC source connector  
- `postgres_log-sink` - PostgreSQL sink connector
- `kafka` - Kafka connector for streaming data
- `kafka-keyed` - Keyed Kafka connector
- `kafka-upsert` - Kafka upsert connector
- `iceberg` - Apache Iceberg connector
- `print` - Print connector for debugging