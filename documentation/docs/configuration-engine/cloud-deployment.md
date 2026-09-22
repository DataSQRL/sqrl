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

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "jobmanager-size": "small",    // Job manager instance size (see table below)
        "taskmanager-size": "medium",  // Task manager instance size (see table below)
        "taskmanager-count": 2,        // Number of task managers (positive integer)
        "taskmanager-disk-size-gb": 400 // NVMe space per task manager (see "Task Manager Disk" below)
      }
    }
  }
}
```

### Task Manager Sizes

| Name   | CPU | Task Slots | Memory (GiB) | NVMe Space | Max CPU Burst |
|:-------|:----|:-----------|:-------------|:-----------|:--------------|
| dev    | 0.5 | 1          | 2            | 20GB       | 2             |
| small  | 1   | 1          | 4            | 55GB       | 1             |
| medium | 2   | 2          | 8            | 110GB      | 1             |
| large  | 4   | 4          | 16           | 220GB      | 1             |
| xlarge | 8   | 8          | 32           | 440GB      | 1             |

The `dev` size is intended for development and testing with small amounts of data.

#### Task Manager Disk

Each task manager gets local NVMe space for RocksDB state, batch spill files and any other Flink-local data. It defaults to the "NVMe Space" column of the selected size, and `taskmanager-disk-size-gb` overrides that default independently of the size:

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "taskmanager-size": "small",     // 55GB of NVMe by default
        "taskmanager-disk-size-gb": 400  // ...raised to 400GB
      }
    }
  }
}
```

Raise it when a job needs more local disk than its CPU/memory size implies — batch jobs in particular spill shuffle and sort data to local disk far beyond their memory footprint, and a task manager that exceeds its allocation is evicted mid-job.

The value is in GiB, must be positive, and is capped at 4000. It is a **hard scheduling requirement**: a task manager asking for more disk than any available node offers stays `Pending` instead of falling back to a smaller node.

#### Size Qualifiers

Task manager sizes support qualifiers for specialized workloads. Qualifiers are grouped into memory-oriented and CPU-oriented variants:

* **`.mem-Nx`** scales the pod memory by `N` and gives Flink **proportionally more** memory (Flink heap+managed grows with `N`). Use for state-heavy jobs.
* **`.mem-headroom-Nx`** scales the pod memory by `N` but keeps Flink's allocation at the **baseline** memory; the extra memory is reserved for sidecar / native consumers (e.g., DuckDB, JNI libs, page cache).
* **`.cpu`** is **deprecated** — it doubles the CPU request *and* the ceiling, which also doubles task slots. Say it precisely with `cpu-request-factor: 2` plus `cpu-limit-factor`; see the migration note below.

| Qualifier          | Pod memory | Flink heap+managed | Typical use                            |
|:-------------------|:-----------|:-------------------|:---------------------------------------|
| `.cpu` (deprecated) | base      | base × 0.80        | Doubles slots too — use the CPU factors |
| `.mem` / `.mem-2x` | base × 2   | base × 1.6         | State-heavy jobs                       |
| `.mem-4x`          | base × 4   | base × 3.2         | Large state                            |
| `.mem-8x`          | base × 8   | base × 6.4         | Very large state                       |
| `.mem-headroom-2x` | base × 2   | base × 1           | Sidecars / native memory consumers     |
| `.mem-headroom-4x` | base × 4   | base × 1           | Larger sidecar headroom                |
| `.mem-headroom-8x` | base × 8   | base × 1           | Maximum sidecar headroom (e.g. DuckDB) |

Examples:

* `medium.mem-4x` → pod 32 GB / Flink heap+managed ≈ 25.6 GB.
* `xlarge.mem-headroom-8x` → pod 256 GB / Flink heap+managed = 32 GB (baseline) / 224 GB headroom.

`.mem` is an alias for `.mem-2x`. The legacy `.mem-headroom` qualifier (triple memory, Flink stays at baseline) is **deprecated** — use `.mem-headroom-Nx` instead.

Qualifiers apply to every size including `dev`: `dev.mem-2x` is a `dev` task manager (0.5 CPU, one task slot) with `small`'s 4 GB of memory.

#### Task Manager CPU Request and Limit Factors

`taskmanager-cpu-request-factor` and `taskmanager-cpu-limit-factor` move the request and the ceiling independently. Both are multiples of the vCPU the size already carries:

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "taskmanager-size": "medium",
        "taskmanager-cpu-request-factor": 0.25,  // request 0.5 cores
        "taskmanager-cpu-limit-factor": 1        // ceiling stays at 2 cores
      }
    }
  }
}
```

Each accepts a number greater than 0 and at most 4. `taskmanager-cpu-request-factor` defaults to `1`; `taskmanager-cpu-limit-factor` defaults to the size's own limit factor. The ceiling must be at least `max(1, taskmanager-cpu-request-factor)`.

**Task slots follow the ceiling, not the request.** Slots scale by the ceiling *relative to the size's own* — `slots x (taskmanager-cpu-limit-factor / the size's default factor)` — because burst headroom with no subtasks to fill it buys nothing. A `medium` (2 slots, default factor 1) at `taskmanager-cpu-limit-factor: 2` gets 4 slots; `dev`'s default factor is already 2, so factor 2 leaves it at 1 slot and factor 4 gives it 2. Lowering `taskmanager-cpu-request-factor` leaves slots alone, which is how you keep the parallelism of a size while sharing its cores at steady state.

Because slots move, so does parallelism (`instances x slots`), and `pipeline.max-parallelism` is baked into savepoints. Raising the limit factor on a running deployment is rejected when the new parallelism no longer divides the recorded `pipeline.max-parallelism`; the error lists the `taskmanager-count` values that do.

### Job Manager Sizes

| Name   | SubTasks | CPU | Memory (GiB) |
|:-------|:---------|:----|:-------------|
| dev    | &lt;100  | 0.5 | 1            |
| small  | 100-800  | 0.5 | 2            |
| medium | 800-2000 | 1   | 4            |
| large  | &gt;2000 | 2   | 8            |

Choose the job manager size based on the number of subtasks in your Flink job.

---

:::warning Migrating from `cpu-limit`
`cpu-limit` and `taskmanager-cpu-limit` are **removed**. A ceiling is now always a factor of the size's own vCPU, so absolute amounts (`"6000m"`) and `"unlimited"` are no longer accepted — replace `"cpu-limit": "4x"` with `"cpu-limit-factor": 4`. A leftover `cpu-limit` is rejected with a message naming its replacement rather than being silently ignored.

The `.cpu` qualifier still works but is **deprecated**. It is equivalent to setting *both* factors — `cpu-request-factor: 2` **and** `cpu-limit-factor: 2` (4 on task-manager `dev`, whose ceiling is already 2x). Setting `cpu-request-factor: 2` on its own is rejected, because the ceiling would then sit below the request. `.cpu` cannot be combined with either factor.

**Migrating `.cpu` on a task manager changes parallelism.** Because slots follow the ceiling, `.cpu` doubles the slots per task manager — `medium.cpu` runs 4 slots, not 2. That is true today as well, so migrating to explicit factors is how you take back control of it: `cpu-request-factor: 2` with `cpu-limit-factor: 1` keeps `medium`'s 2 slots while still reserving 4 cores.
:::

## PostgreSQL (`engines.postgres.deployment`)

PostgreSQL deployments consist of one primary instance and a configurable number of read replicas, all using the same instance size.

```json
{
  "engines": {
    "postgres": {
      "deployment": {
        "instance-size": "medium",      // Instance size (see table below)
        "cpu-limit-factor": 3,          // CPU ceiling (see "CPU Request and Limit Factors")
        "replica-count": 1,             // Number of read replicas (0 or larger)
        "disk-size-gb": 256,            // Disk size in GB (1 or larger)
        "auto-expand-percentage": 0.2,  // Auto-expand threshold (0 to disable, must be < 1)
        "create-indexes": true,         // Whether to create table indexes (see "Create Indexes" below)
        "data-checksums": true,         // Whether data-page checksums are enabled (see "Data Checksums" below)
        "parameters": {}                // Extra postgresql.parameters (see "Parameters" below)
      }
    }
  }
}
```

### Instance Sizes

| Name   | CPU | Memory (GiB) | Default Disk | Max CPU Burst | Max Connections |
|:-------|:----|:-------------|:-------------|:--------------|:----------------|
| dev    | 0.5 | 2            | 10GB         | 1.5           | 100             |
| small  | 1   | 4            | 128GB        | 1             | 100             |
| medium | 2   | 8            | 256GB        | 1             | 200             |
| large  | 4   | 16           | 512GB        | 1             | 300             |
| xlarge | 8   | 32           | 1TB          | 1             | 600             |

The `dev` size is intended for development and testing with small amounts of data.

### Size Qualifiers

Instance sizes accept the same `.mem-Nx` qualifiers as task managers, which is how a database asks for memory without the cores the size would otherwise bring:

* `small.mem-4x` → 1 CPU, 16 GiB — the memory of `large` at a quarter of its CPU request.

A memory qualifier moves memory only — CPU is moved by `cpu-request-factor`. A qualifier also leaves `max_connections` and the default disk size at the base size's values. The `.cpu` qualifier is **deprecated** and cannot be combined with either CPU factor.

### CPU Request and Limit Factors

Every size fixes CPU and memory together at 4 GB per core, so a component sized for its memory carries more CPU request than it needs. Two factors move the request and the ceiling independently, and **both are multiples of the vCPU the size already carries** — not of each other:

| Setting | Range | Default | Effect |
|:--------|:------|:--------|:-------|
| `cpu-request-factor` | 0 (exclusive) to 4 | `1` | `request = size vCPU x factor` |
| `cpu-limit-factor`   | 1 to 4 | the size's own limit factor | `limit = size vCPU x factor` |

For a `medium` task manager (2 vCPU):

| Factors | Request | Limit | Meaning |
|:--------|:--------|:------|:--------|
| request `0.25`, limit `1` | 0.5 | 2 | share cores at steady state, keep the full ceiling |
| request `1`, limit `2`    | 2   | 4 | reserve the size, burst to double |
| request `0.25`, limit `4` | 0.5 | 8 | reserve little, burst hard |

`cpu-limit-factor` must be at least `max(1, cpu-request-factor)`, otherwise the ceiling would fall below the request and Kubernetes rejects the pod. Any ceiling above the request makes the pod Burstable rather than Guaranteed, which lowers its eviction priority under node pressure; a request factor below 1 reserves less than the size and shares cores with neighbours at steady state.

---

## Vert.x (`engines.vertx.deployment`)

Vert.x API server deployments consist of a configurable number of identically sized server instances.

```json
{
  "engines": {
    "vertx": {
      "deployment": {
        "instance-size": "small",  // Instance size (see table below)
        "instance-count": 2,       // Number of server instances (positive integer)
        "cpu-limit-factor": 4      // CPU ceiling (see "CPU Request and Limit Factors")
      }
    }
  }
}
```

### Instance Sizes

| Name   | CPU  | Memory (GiB) | Max CPU Burst | Pg Pool Size |
|:-------|:-----|:-------------|:--------------|:-------------|
| dev    | 0.25 | 1            | 1.25          | 5            |
| small  | 0.5  | 2            | 1             | 5            |
| medium | 1    | 4            | 1             | 10           |
| large  | 2    | 8            | 1             | 15           |
| xlarge | 4    | 16           | 1             | 20           |

The `dev` size is intended for development and testing with small amounts of data. The `.disk` qualifier enables NVMe storage for instances that require local disk access.

### Size Qualifiers

Server sizes accept the `.mem-Nx` qualifiers as well, and they compose with `.disk`:

* `dev.mem-2x` → 0.25 CPU, 2 GiB — `small`'s memory at half its CPU request.
* `small.disk.mem-2x` → 0.5 CPU, 4 GiB.

### CPU Request and Limit Factors

`cpu-request-factor` and `cpu-limit-factor` behave exactly as for PostgreSQL above: each is a number greater than 0 and at most 4, measured against the vCPU the size already carries, with `cpu-limit-factor` at least `max(1, cpu-request-factor)`.

---

## Dedicated Nodes (`*-dedicated-nodes`)

Pins a component's pods onto dedicated nodes. Each engine's `deployment` accepts a list of dedicated-node names. Each name is a **hard requirement**: if no matching node is available, the pod stays `Pending` — it never falls back to a shared node.

| Engine     | Field(s)                                                    |
|:-----------|:------------------------------------------------------------|
| Flink      | `taskmanager-dedicated-nodes`, `jobmanager-dedicated-nodes` |
| PostgreSQL | `dedicated-nodes`                                           |
| Vert.x     | `dedicated-nodes`                                           |

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "taskmanager-size": "medium.mem",
        "taskmanager-count": 6,
        "taskmanager-dedicated-nodes": [ "nvme" ]   // TaskManagers MUST run on the "nvme" dedicated nodes
      }
    }
  }
}
```

For each name `N` in the list, the pod is given:

* a **node selector** requiring the node label `N=true` (the pin), and
* a **toleration** for taint key `N` (so it is allowed onto the dedicated, tainted nodes).

The cluster side is an infrastructure concern: the dedicated nodes for `N` must be labeled `N=true` **and** tainted `N=<value>:NoSchedule`. The taint keeps everything that does not request `N` off those nodes; the matching label + toleration place the requesting pods on them. This gives both *pinning* (the workload runs there) and *isolation* (nothing else does).

Multiple names are combined with AND — the pod requires a node carrying all of them.

### Provisioning the dedicated nodes (cluster side)

Dedicated nodes are provisioned by the cluster operator, not by the pipeline. The contract is simple: the nodes for `N` must carry the **label** `N=true` and the **taint** `N=true:NoSchedule`. The label provides the pin (pods requesting `N` land here); the taint provides the isolation (everything that does not request `N` is kept off).

On a [Karpenter](https://karpenter.sh/)-managed cluster (e.g. Amazon EKS), create a `NodePool`. For a group named `nvme` backed by local-NVMe instances:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: nvme
spec:
  template:
    metadata:
      labels:
        nvme: "true"            # selected by pods requesting dedicated nodes "nvme"
    spec:
      taints:
        - key: nvme             # repels everything that does not tolerate "nvme"
          value: "true"
          effect: NoSchedule
      requirements:
        - { key: node.kubernetes.io/instance-type, operator: In, values: [ i7i.2xlarge ] }
        - { key: karpenter.sh/capacity-type, operator: In, values: [ on-demand ] }
      # nodeClassRef, arch/os, disruption budgets and limits as appropriate for your cluster
```

The label name and taint key must both equal the dedicated-nodes name. These nodes must also carry any standard scheduling labels your platform applies to workload nodes, so the deployment's base node selection still resolves.

---

## Do Not Disrupt (`do-not-disrupt`)

Protects a component's pods from **voluntary** autoscaler disruption (node consolidation / scale-down). When `true`, the pods are annotated so the cluster autoscaler will not evict or consolidate them. Use it for long-running, stateful, or hard-to-reschedule workloads — for example a Flink catch-up that reprocesses the whole backlog, or the PostgreSQL primary during bootstrap.

| Engine     | Field            | Default |
|:-----------|:-----------------|:--------|
| Flink      | `do-not-disrupt` | `false` |
| PostgreSQL | `do-not-disrupt` | `true`  |
| Vert.x     | `do-not-disrupt` | `false` |

```json
{
  "engines": {
    "flink": {
      "deployment": {
        "do-not-disrupt": true   // keep TaskManagers/JobManager from being consolidated mid-run
      }
    }
  }
}
```

---

## Schedule (`schedule`)

Runs a pipeline as a **scheduled batch job**: the Flink cluster is created at each fire time, runs the job to completion, and is torn down again. Flink only; absent by default.

| Engine | Field      | Default |
|:-------|:-----------|:--------|
| Flink  | `schedule` | absent  |

Without a `schedule`, what the deployment does is decided by `execution.runtime-mode` alone: a `STREAMING` pipeline (the default) runs continuously, while a `BATCH` pipeline runs once and then stays dormant until it is deployed again.

```json
{
  "engines": {
    "flink": {
      "config": {
        "execution.runtime-mode": "BATCH"   // required: a schedule only takes effect for batch jobs
      },
      "deployment": {
        "schedule": {
          "cron": "0 3 * * *",              // daily at 03:00
          "timezone": "America/New_York"
        }
      }
    }
  }
}
```

Both fields are required when `schedule` is present:

* `cron`: a 5-field UNIX cron expression — `minute hour day-of-month month day-of-week`. There is no seconds field, so the shortest interval is one minute.
* `timezone`: an [IANA timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) name, for example `UTC` or `America/New_York`. Fire times are computed in that zone, so a schedule follows the zone's daylight-saving shifts instead of a fixed UTC offset.

Both are validated before anything is deployed: an unparseable cron expression or an unknown timezone fails the deployment with an error naming the offending value.

`"execution.runtime-mode": "BATCH"` is required for the schedule to take effect. A streaming job never finishes, so it never releases the cluster and no fire time is ever reached.

### Run Cycle

A scheduled deployment alternates between running and dormant:

1. **Run** — at the fire time the Flink cluster is created and the batch job processes the data currently available in its sources.
2. **Sleep** — when the job reaches a terminal state the Flink cluster is removed, and the deployment reports as dormant. The database and the API stay up and keep serving the results of the last run; no Flink resources are consumed between runs.
3. **Wake** — the next fire time is computed from the moment the run ended, and the cluster is recreated then.

Because the next fire time is derived from the end of the previous run, runs never overlap. A run that takes longer than its interval pushes the following fire times out; occurrences that pass while the job is still running are skipped, not queued.

---

## Create Indexes (`create-indexes`)

Controls whether the PostgreSQL table indexes are created for the deployment. Defaults to `true`. PostgreSQL only.

| Engine     | Field            | Default |
|:-----------|:-----------------|:--------|
| PostgreSQL | `create-indexes` | `true`  |

Set it to `false` to bootstrap the database **tables-only**, skipping all index creation. This is intended for a catch-up profile that reprocesses a large backlog: writing to un-indexed tables drains the backlog faster. The indexes are then built when the deployment is upgraded back to a steady-state profile (where `create-indexes` returns to its `true` default), so a catch-up deployment must be followed by such an upgrade before it serves production query traffic.

```json
{
  "engines": {
    "postgres": {
      "deployment": {
        "create-indexes": false   // tables-only bootstrap for fast backlog draining; build indexes on the steady-state upgrade
      }
    }
  }
}
```

## Data Checksums (`data-checksums`)

Controls whether PostgreSQL data-page checksums are enabled for the database. Defaults to `true` (the PostgreSQL 18 default). PostgreSQL only.

| Engine     | Field            | Default |
|:-----------|:-----------------|:--------|
| PostgreSQL | `data-checksums` | `true`  |

This is an initdb-time setting applied when the database is first created — it is **immutable** and cannot be changed on later deployments or upgrades. Set it to `false` only when the write-throughput cost of checksums matters more than corruption detection, and only for a database that will keep that setting for its lifetime.

```json
{
  "engines": {
    "postgres": {
      "deployment": {
        "data-checksums": false   // disable data-page checksums; applied at initial bootstrap only, immutable afterwards
      }
    }
  }
}
```

## Parameters (`parameters`)

Extra PostgreSQL server parameters, merged into the database's `postgresql.parameters`. Any key here overrides the built-in default for that parameter. PostgreSQL only; empty by default.

| Engine     | Field        | Default |
|:-----------|:-------------|:--------|
| PostgreSQL | `parameters` | `{}`    |

Typically used in a catch-up profile that trades durability for ingest throughput while reprocessing a large backlog — for example a larger `shared_buffers`/`max_wal_size` together with `synchronous_commit: off`. Set these only in the catch-up profile: on the steady-state upgrade any parameter not listed here reverts to its default.

```json
{
  "engines": {
    "postgres": {
      "deployment": {
        "parameters": {
          "shared_buffers": "8GB",        // override the default (256MB)
          "max_wal_size": "32GB",
          "synchronous_commit": "off"     // faster backfill, durability trade-off
        }
      }
    }
  }
}
```

For deployments with [partitioned tables](postgres.md#partitioning), `pg_partman_bgw.interval` (seconds between pg_partman background-worker maintenance runs, default `3600`) can also be overridden here. The `pg_partman_bgw.dbname` and `pg_partman_bgw.role` settings are managed by the platform and cannot be overridden.

