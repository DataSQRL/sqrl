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

Refer to the [Flink Documentation](hhttps://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/config/) for all Flink configuration options.

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

## Deployment Configuration

Flink supports deployment-specific configuration options for managing cluster resources:

| Key                 | Type        | Default | Description                                                     |
|---------------------|-------------|---------|-----------------------------------------------------------------|
| `jobmanager-size`   | **string**  | -       | Job manager instance size: `dev`, `small`, `medium`, `large`    |
| `taskmanager-size`  | **string**  | -       | Task manager instance size with resource variants               |
| `taskmanager-count` | **integer** | -       | Number of task manager instances (minimum: 1)                   |
| `secrets`           | **array**   | `null`  | Array of secret names to inject, or `null` if no secrets needed |

### Task Manager Size Options

Available `taskmanager-size` options with resource variants:
- `dev` - Development/testing size
- `small`, `small.mem`, `small.cpu` - Small instances with memory or CPU optimization
- `medium`, `medium.mem`, `medium.cpu` - Medium instances with resource variants  
- `large`, `large.mem`, `large.cpu` - Large instances with resource variants

### Deployment Example

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "jobmanager-size": "small",
        "taskmanager-size": "medium.mem", 
        "taskmanager-count": 2,
        "secrets": ["flink-secrets", "db-credentials"]
      }
    }
  }
}
```