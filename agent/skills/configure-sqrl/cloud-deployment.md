# Cloud Deployment Configuration

Use `engines.<engine>.deployment` only for DataSQRL Cloud resources. These fields are cloud deployment settings, not local Flink, Postgres, or Vert.x runtime settings.

## Flink (`engines.flink.deployment`)

Flink has one JobManager and one or more identically sized TaskManagers.

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "jobmanager-size": "small",
        "taskmanager-size": "medium.mem-4x",
        "taskmanager-count": 2,
        "taskmanager-disk-size-gb": 400
      }
    }
  }
}
```

| TaskManager size | CPU | Slots | Memory | Default NVMe |
|---|---:|---:|---:|---:|
| `dev` | 0.5 | 1 | 2 GiB | 20 GB |
| `small` | 1 | 1 | 4 GiB | 55 GB |
| `medium` | 2 | 2 | 8 GiB | 110 GB |
| `large` | 4 | 4 | 16 GiB | 220 GB |
| `xlarge` | 8 | 8 | 32 GiB | 440 GB |

`taskmanager-disk-size-gb` independently overrides the default local-NVMe allocation. It must be positive and no greater than 4000 GiB. Treat it as a hard scheduling requirement: an unavailable requested disk size leaves the pod Pending.

Size qualifiers apply to every size, including `dev`:

| Qualifier | Effect | Use for |
|---|---|---|
| `.cpu` | doubles CPU; baseline memory | CPU-bound work |
| `.mem` or `.mem-2x` | doubles pod memory; Flink heap + managed memory scale proportionally | state-heavy jobs |
| `.mem-4x`, `.mem-8x` | multiplies memory by 4 or 8 | very large state |
| `.mem-headroom-2x`, `.mem-headroom-4x`, `.mem-headroom-8x` | adds pod memory while retaining Flink's baseline allocation | DuckDB sidecars, JNI, or native/page-cache needs |

The legacy `.mem-headroom` form is deprecated; use an explicit `-Nx` qualifier. `medium.mem-4x`, for example, requests 32 GiB of pod memory.

| JobManager size | Expected subtasks | CPU | Memory |
|---|---:|---:|---:|
| `dev` | under 100 | 0.5 | 1 GiB |
| `small` | 100–800 | 0.5 | 2 GiB |
| `medium` | 800–2000 | 1 | 4 GiB |
| `large` | over 2000 | 2 | 8 GiB |

### Scheduled batch jobs

Use a schedule only with a batch pipeline. A scheduled run creates the Flink cluster at each fire time and removes it after the job completes; a streaming job never reaches the next fire time.

```json
{
  "engines": {
    "flink": {
      "config": { "execution.runtime-mode": "BATCH" },
      "deployment": {
        "schedule": { "cron": "0 3 * * *", "timezone": "America/New_York" }
      }
    }
  }
}
```

`cron` is a five-field UNIX expression and `timezone` is an IANA timezone. Both are required. Runs never overlap; an occurrence while the previous run is active is skipped.

## PostgreSQL (`engines.postgres.deployment`)

Postgres has one primary and zero or more same-sized read replicas.

```json
{
  "engines": {
    "postgres": {
      "deployment": {
        "instance-size": "medium",
        "replica-count": 1,
        "disk-size-gb": 256,
        "auto-expand-percentage": 0.2,
        "create-indexes": true,
        "data-checksums": true,
        "parameters": {}
      }
    }
  }
}
```

| Size | CPU | Memory | Default disk | Connections |
|---|---:|---:|---:|---:|
| `dev` | 0.5 | 2 GiB | 10 GB | 100 |
| `small` | 1 | 4 GiB | 128 GB | 100 |
| `medium` | 2 | 8 GiB | 256 GB | 200 |
| `large` | 4 | 16 GiB | 512 GB | 300 |
| `xlarge` | 8 | 32 GiB | 1 TB | 600 |

`replica-count` is zero or greater. `disk-size-gb` must be positive; `auto-expand-percentage` is zero to disable and otherwise less than one. `create-indexes` defaults to `true`; set it to `false` only for a catch-up deployment, then upgrade to a steady-state profile that creates indexes before serving production traffic. `data-checksums` defaults to `true` and is immutable after database creation. `parameters` overrides PostgreSQL server parameters, typically only for an explicit catch-up profile.

## Vert.x (`engines.vertx.deployment`)

```json
{
  "engines": {
    "vertx": {
      "deployment": {
        "instance-size": "small",
        "instance-count": 2
      }
    }
  }
}
```

| Size | CPU | Memory | Pg pool size |
|---|---:|---:|---:|
| `dev` | 0.25 | 1 GiB | 5 |
| `small` | 0.5 | 2 GiB | 5 |
| `medium` | 1 | 4 GiB | 10 |
| `large` | 2 | 8 GiB | 15 |
| `xlarge` | 4 | 16 GiB | 20 |

`instance-count` must be positive. The `.disk` qualifier enables local NVMe when needed.

## Dedicated nodes and disruption protection

Dedicated-node names are hard scheduling requirements: the workload remains Pending if no matching node exists. Use `taskmanager-dedicated-nodes` and `jobmanager-dedicated-nodes` for Flink, or `dedicated-nodes` for Postgres and Vert.x. Each name requires the cluster operator to label matching nodes `<name>=true` and taint them with `<name>=true:NoSchedule`.

`do-not-disrupt` prevents voluntary autoscaler disruption. It defaults to `false` for Flink and Vert.x and `true` for Postgres. Use it for stateful, long-running, or hard-to-reschedule work.

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "taskmanager-dedicated-nodes": ["nvme"],
        "do-not-disrupt": true
      }
    }
  }
}
```
