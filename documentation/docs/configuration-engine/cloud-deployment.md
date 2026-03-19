# Cloud Deployment Configuration

Configures the cloud resources for each engine when deploying pipelines to DataSQRL Cloud. Deployment settings are configured under the `deployment` field within each engine's configuration.

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "jobmanager-size": "small",
        "taskmanager-size": "large.mem",
        "taskmanager-count": 1
      },
      "config": {
        "execution.checkpointing.interval": "180s"
      }
    },
    "postgres": {
      "deployment": {
        "instance-size": "medium",
        "replica-count": 1
      }
    },
    "vertx": {
      "deployment": {
        "instance-size": "small",
        "instance-count": 1
      }
    }
  }
}
```

---

## Flink (`engines.flink.deployment`)

Apache Flink deployments consist of 1 job manager and a configurable number of identically sized task managers.

```json5
{
  "engines": {
    "flink": {
      "deployment": {
        "jobmanager-size": "small",   // Job manager instance size (see table below)
        "taskmanager-size": "medium", // Task manager instance size (see table below)
        "taskmanager-count": 2        // Number of task managers (positive integer)
      }
    }
  }
}
```

### Task Manager Sizes

| Name | CPU | Task Slots | Memory (GiB) | NVMe Space | Max CPU Burst |
| :--- | :--- | :--- | :--- | :--- | :--- |
| dev | 0.5 | 1 | 2 | 20GB | 2 |
| small | 1 | 1 | 4 | 55GB | 1 |
| medium | 2 | 2 | 8 | 110GB | 1 |
| large | 4 | 4 | 16 | 220GB | 1 |
| xlarge | 8 | 8 | 32 | 440GB | 1 |

The `dev` size is intended for development and testing with small amounts of data.

#### Size Qualifiers

Task manager sizes support qualifiers for specialized workloads:

* **`.mem`**: Doubles memory (8x CPU). Use for state-heavy jobs. Example: `medium.mem` provides 16GB instead of 8GB.
* **`.cpu`**: Doubles CPU with same memory. Use for CPU-intensive jobs. Example: `medium.cpu` provides 4 CPU instead of 2.
* **`.mem-headroom`**: Triples memory (12x CPU). Use when running sidecar processes that require significant memory (e.g., DuckDB for query execution).

Size qualifiers do not apply to the `dev` instance.

### Job Manager Sizes

| Name | SubTasks | CPU | Memory (GiB) |
| :--- | :--- | :--- | :--- |
| dev | &lt;100 | 0.5 | 1 |
| small | 100-800 | 0.5 | 2 |
| medium | 800-2000 | 1 | 4 |
| large | &gt;2000 | 2 | 8 |

Choose the job manager size based on the number of subtasks in your Flink job.

---

## PostgreSQL (`engines.postgres.deployment`)

PostgreSQL deployments consist of one primary instance and a configurable number of read replicas, all using the same instance size.

```json5
{
  "engines": {
    "postgres": {
      "deployment": {
        "instance-size": "medium",      // Instance size (see table below)
        "replica-count": 1,             // Number of read replicas (0 or larger)
        "disk-size-gb": 256,            // Disk size in GB (1 or larger)
        "auto-expand-percentage": 0.2   // Auto-expand threshold (0 to disable, must be < 1)
      }
    }
  }
}
```

### Instance Sizes

| Name | CPU | Memory (GiB) | Default Disk | Max CPU Burst | Max Connections |
| :--- | :--- | :--- | :--- | :--- | :--- |
| dev | 0.5 | 4 | 10GB | 1.5 | 100 |
| small | 1 | 8 | 128GB | 1 | 100 |
| medium | 2 | 16 | 256GB | 1 | 200 |
| large | 4 | 16 | 512GB | 1 | 300 |
| xlarge | 8 | 32 | 1TB | 1 | 600 |

The `dev` size is intended for development and testing with small amounts of data.

---

## Vert.x (`engines.vertx.deployment`)

Vert.x API server deployments consist of a configurable number of identically sized server instances.

```json5
{
  "engines": {
    "vertx": {
      "deployment": {
        "instance-size": "small",  // Instance size (see table below)
        "instance-count": 2        // Number of server instances (positive integer)
      }
    }
  }
}
```

### Instance Sizes

| Name | CPU | Memory (GiB) | NVMe Space | Max CPU Burst | Pg Pool Size |
| :--- | :--- | :--- | :--- | :--- | :--- |
| dev | 0.5 | 2 | - | 1.25 | 5 |
| small | 1 | 4 | 55GB | 1 | 5 |
| medium | 2 | 8 | 110GB | 1 | 10 |
| large | 4 | 16 | 220GB | 1 | 15 |

The `dev` size is intended for development and testing with small amounts of data. The `.disk` qualifier enables NVMe storage for instances that require local disk access.