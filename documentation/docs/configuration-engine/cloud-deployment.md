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

| Name    | CPU | Task Slots | Memory (GiB) | NVMe Space | Max CPU Burst |
|:--------|:----|:-----------|:-------------|:-----------|:--------------|
| dev     | 0.5 | 1          | 2            | 20GB       | 2             |
| small   | 1   | 1          | 4            | 55GB       | 1             |
| medium  | 2   | 2          | 8            | 110GB      | 1             |
| large   | 4   | 4          | 16           | 220GB      | 1             |
| xlarge  | 8   | 8          | 32           | 440GB      | 1             |
| xxlarge | 16  | 16         | 64           | 880GB      | 1             |

The `dev` size is intended for development and testing with small amounts of data.

"Max CPU Burst" is the size's own limit factor — the CPU ceiling it gets when [`taskmanager-cpu-limit-factor`](#cpu-request-and-limit-factors) is not set. `taskmanager-size` also accepts [size qualifiers](#size-qualifiers), as in `medium.mem-4x`.

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

### Job Manager Sizes

| Name   | SubTasks | CPU | Max CPU Burst | Memory (GiB) |
|:-------|:---------|:----|:--------------|:-------------|
| dev    | &lt;100  | 0.5 | 2             | 1            |
| small  | 100-800  | 0.5 | 2             | 2            |
| medium | 801-2000 | 1   | 2             | 4            |
| large  | &gt;2000 | 2   | 2             | 8            |

Choose the job manager size based on the number of subtasks in your Flink job. `jobmanager-size` takes a bare size name without [size qualifiers](#size-qualifiers); its CPU request and ceiling move with [`jobmanager-cpu-request-factor` and `jobmanager-cpu-limit-factor`](#cpu-request-and-limit-factors).

---

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

### Database Instance Sizes

| Name   | CPU | Memory (GiB) | Default Disk | Max CPU Burst | Max Connections |
|:-------|:----|:-------------|:-------------|:--------------|:----------------|
| dev    | 0.5 | 2            | 10GB         | 1.5           | 100             |
| small  | 1   | 4            | 128GB        | 1             | 100             |
| medium | 2   | 8            | 256GB        | 1             | 200             |
| large  | 4   | 16           | 512GB        | 1             | 300             |
| xlarge | 8   | 32           | 1TB          | 1             | 600             |

The `dev` size is intended for development and testing with small amounts of data.

`instance-size` accepts [size qualifiers](#size-qualifiers), which is how a database asks for memory without the cores the size would otherwise bring: `small.mem-4x` is 1 CPU with 16 GiB, the memory of `large` at a quarter of its CPU request. `max_connections` and the default disk size always stay at the base size's values.

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

### Server Instance Sizes

| Name   | CPU  | Memory (GiB) | Max CPU Burst | Pg Pool Size |
|:-------|:-----|:-------------|:--------------|:-------------|
| dev    | 0.25 | 1            | 1.25          | 5            |
| small  | 0.5  | 2            | 1             | 5            |
| medium | 1    | 4            | 1             | 10           |
| large  | 2    | 8            | 1             | 15           |
| xlarge | 4    | 16           | 1             | 20           |

The `dev` size is intended for development and testing with small amounts of data.

`instance-size` accepts [size qualifiers](#size-qualifiers): `dev.mem-2x` is 0.25 CPU with 2 GiB, `small`'s memory at half its CPU request. Each tier is half the one above it, so a server that only holds connections open is not forced onto a core it will not use.

---

## Size Qualifiers

A size name can carry qualifiers, written after it and separated by dots — `medium.mem-4x`. A memory qualifier scales the **pod's memory** and nothing else: CPU, `max_connections` and the default disk size all stay at the base size's values. The one exception is `.cpu`, which moves CPU rather than memory and is **deprecated** — the [CPU factors](#cpu-request-and-limit-factors) say the same thing precisely.

| Setting                                     | Qualifiers                                          |
|:--------------------------------------------|:----------------------------------------------------|
| `engines.flink.deployment.taskmanager-size` | `.mem-Nx`, `.mem-headroom-Nx`, `.cpu` (deprecated)  |
| `engines.postgres.deployment.instance-size` | `.mem-Nx`, `.mem-headroom-Nx`, `.cpu` (deprecated)  |
| `engines.vertx.deployment.instance-size`    | `.mem-Nx`, `.mem-headroom-Nx`, `.cpu` (deprecated)  |
| `engines.flink.deployment.jobmanager-size`  | none — a bare size name                              |

| Qualifier          | Pod memory | Flink heap+managed | Typical use                            |
|:-------------------|:-----------|:-------------------|:---------------------------------------|
| `.mem` / `.mem-2x` | base × 2   | base × 1.8         | State-heavy jobs                       |
| `.mem-4x`          | base × 4   | base × 3.6         | Large state                            |
| `.mem-8x`          | base × 8   | base × 7.2         | Very large state                       |
| `.mem-headroom-2x` | base × 2   | base × 1           | Sidecars / native memory consumers     |
| `.mem-headroom-4x` | base × 4   | base × 1           | Larger sidecar headroom                |
| `.mem-headroom-8x` | base × 8   | base × 1           | Maximum sidecar headroom (e.g. DuckDB) |

The "Flink heap+managed" column applies to task managers only. `.mem-Nx` gives Flink **proportionally more** memory, while `.mem-headroom-Nx` keeps Flink's allocation at the **baseline** and reserves the extra for sidecar and native consumers (DuckDB, JNI buffers, page cache). For PostgreSQL and Vert.x a qualifier simply scales the memory request and limit.

Examples:

* `medium.mem-4x` → pod 32 GB / Flink heap+managed ≈ 28.8 GB.
* `xlarge.mem-headroom-8x` → pod 256 GB / Flink heap+managed = 32 GB (baseline) / 224 GB headroom.

At most one memory qualifier may be named; naming two is rejected rather than letting the last one win. `general` is accepted and means "no qualifier". Qualifiers apply to every size including `dev`: `dev.mem-2x` is a `dev` task manager (0.5 CPU, one task slot) with `small`'s 4 GB of memory.

`.mem` is an alias for `.mem-2x`. The legacy `.mem-headroom` qualifier (triple memory, Flink stays at baseline) is **deprecated** — use `.mem-headroom-Nx` instead. For `.cpu`, see the migration note below.

---

## CPU Request and Limit Factors

Sizes fix CPU and memory together at 4 GiB per core (the job manager's `dev`, at 2 GiB, is the one exception), so a component sized for its memory carries more CPU request than it needs. Two factors move the request and the ceiling independently, and **both are multiples of the vCPU the size already carries** — not of each other:

| Engine             | Request                          | Ceiling                        |
|:-------------------|:---------------------------------|:-------------------------------|
| Flink task manager | `taskmanager-cpu-request-factor` | `taskmanager-cpu-limit-factor` |
| Flink job manager  | `jobmanager-cpu-request-factor`  | `jobmanager-cpu-limit-factor`  |
| PostgreSQL         | `cpu-request-factor`             | `cpu-limit-factor`             |
| Vert.x             | `cpu-request-factor`             | `cpu-limit-factor`             |

| Setting        | Range                    | Default                        | Effect                        |
|:---------------|:-------------------------|:-------------------------------|:------------------------------|
| request factor | greater than 0, at most 4 | `1`                           | `request = size vCPU x factor` |
| limit factor   | greater than 0, at most 4 | the size's "Max CPU Burst"     | `limit = size vCPU x factor`   |

For a `medium` task manager (2 vCPU):

| Factors | Request | Limit | Meaning |
|:--------|:--------|:------|:--------|
| request `0.25`, limit `1` | 0.5 | 2 | share cores at steady state, keep the full ceiling |
| request `1`, limit `2`    | 2   | 4 | reserve the size, burst to double |
| request `0.25`, limit `4` | 0.5 | 8 | reserve little, burst hard |

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

The limit factor must be at least `max(1, request factor)`, otherwise the ceiling would fall below the request and Kubernetes rejects the pod; the deployment fails with a message naming both settings. Any ceiling above the request makes the pod Burstable rather than Guaranteed, which lowers its eviction priority under node pressure; a request factor below 1 reserves less than the size and shares cores with neighbours at steady state.

### Task Slots Follow the Ceiling

On a Flink task manager the slot count moves with the **ceiling**, not the request, because burst headroom with no subtasks to fill it buys nothing. Slots scale by the ceiling *relative to the size's own*: `slots x (taskmanager-cpu-limit-factor / the size's Max CPU Burst)`. A `medium` (2 slots, burst 1) at `taskmanager-cpu-limit-factor: 2` gets 4 slots; `dev`'s burst is already 2, so factor 2 leaves it at 1 slot and factor 4 gives it 2. Lowering `taskmanager-cpu-request-factor` leaves slots alone, which is how you keep the parallelism of a size while sharing its cores at steady state. The job manager runs no subtasks, so its factors never move slots or parallelism.

Because slots move, so does parallelism (`instances x slots`), and `pipeline.max-parallelism` is baked into savepoints. Raising the limit factor on a running deployment is rejected when the new parallelism no longer divides the recorded `pipeline.max-parallelism`; the error lists the `taskmanager-count` values that do.

:::warning Migrating from `cpu-limit`
`taskmanager-cpu-limit` (Flink) and `cpu-limit` (PostgreSQL) are **deprecated but still accepted**; a later release removes them. Vert.x never had either key. A deployment that still sets one keeps deploying: the value is translated into the matching limit factor and logged as deprecated.

| Old value                         | Translated to          |
|:----------------------------------|:-----------------------|
| `"2x"`                            | `cpu-limit-factor: 2`  |
| `"6000m"` on a `medium` (2 vCPU)  | `cpu-limit-factor: 3`  |
| `"6"` on a `medium`               | `cpu-limit-factor: 3`  |

An absolute amount is divided by the size's own vCPU — `m` means millicores, a bare number means cores. The translation fails instead of deploying on `"unlimited"`, on a value that is not a number, on one that works out above 4, and when the old key is set alongside `cpu-request-factor` or `cpu-limit-factor`. Set one or the other.

**The two keys do not mean the same thing.** `cpu-limit` stated the ceiling as a multiple of the **request**; `cpu-limit-factor` states it against the **size**. Those agree only while the request equals the size — which is exactly what a configuration written before `cpu-request-factor` existed does, so the translation is faithful today and diverges the moment you lower the request.

**On a task manager the translation also moves task slots.** Slots follow the ceiling, so a deployment carrying `taskmanager-cpu-limit: "2x"` on a `medium` goes from 2 slots to 4 and doubles its parallelism — an upgrade whose new parallelism no longer divides the savepoint's `pipeline.max-parallelism` is rejected. Migrate deliberately rather than letting the translation move it for you.

The `.cpu` qualifier still works but is **deprecated**. It is equivalent to setting *both* factors — `cpu-request-factor: 2` **and** a limit factor at twice the size's Max CPU Burst, so 2 for most sizes and 4 on a `dev` task manager. Setting `cpu-request-factor: 2` on its own is rejected on any size whose Max CPU Burst is below 2, because the ceiling would then sit below the request. `.cpu` cannot be combined with either factor.

**Migrating `.cpu` on a task manager changes parallelism.** Because slots follow the ceiling, `.cpu` doubles the slots per task manager — `medium.cpu` runs 4 slots, not 2. Explicit factors do not undo that: a request factor of 2 forces a ceiling of at least 2, and the slots follow. What they add is the other half of the trade, which `.cpu` could never express — `taskmanager-cpu-request-factor: 0.25` with `taskmanager-cpu-limit-factor: 1` keeps `medium`'s 2 slots and shares its cores at steady state.
:::

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
