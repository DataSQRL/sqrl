# Flink Engine Configuration

Apache Flink is a streaming and batch data processor that serves as the core data processing engine in DataSQRL pipelines.

## Configuration Options

| Key          | Type       | Default   | Notes                                                                                              |
|--------------|------------|-----------|----------------------------------------------------------------------------------------------------| 
| `config`     | **object** | see below | Copied verbatim into the generated Flink SQL job (e.g. `"table.exec.source.idle-timeout": "5 s"`). |

Frequently configured options include:

* `execution.runtime-mode`: `BATCH` or `STREAMING`
* `table.exec.source.idle-timeout`: Timeout for idle sources so watermark can advance.
* `table.exec.mini-batch.*`: For more efficient execution in STREAMING mode by processing in small batches.

Refer to the [Flink Documentation](hhttps://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/config/) for all Flink configuration options.

## Example Configuration

```json
{
  "engines": {
    "flink": {
      "config": {
        "execution.runtime-mode": "STREAMING",
        "rest.port": 8081,
        "state.backend.type": "rocksdb",
        "table.exec.resource.default-parallelism": 1,
        "taskmanager.memory.network.max": "800m"
      }
    }
  }
}
```

## Cloud Deployment

For cloud deployment configuration (instance sizes, task manager counts, scheduling), see [Cloud Deployment Configuration](cloud-deployment.md#flink-enginesflinkdeployment).
