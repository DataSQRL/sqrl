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

Task manager sizes support qualifiers for specialized workloads. Qualifiers are grouped into memory-oriented and CPU-oriented variants:

* **`.mem-Nx`** scales the pod memory by `N` and gives Flink **proportionally more** memory (Flink heap+managed grows with `N`). Use for state-heavy jobs.
* **`.mem-headroom-Nx`** scales the pod memory by `N` but keeps Flink's allocation at the **baseline** memory; the extra memory is reserved for sidecar / native consumers (e.g., DuckDB, JNI libs, page cache).
* **`.cpu`** doubles CPU with the same memory.

| Qualifier | Pod memory | Flink heap+managed | Typical use |
| :--- | :--- | :--- | :--- |
| `.cpu` | base | base × 0.80 | CPU-intensive jobs |
| `.mem` / `.mem-2x` | base × 2 | base × 1.6 | State-heavy jobs |
| `.mem-4x` | base × 4 | base × 3.2 | Large state |
| `.mem-8x` | base × 8 | base × 6.4 | Very large state |
| `.mem-headroom-2x` | base × 2 | base × 1 | Sidecars / native memory consumers |
| `.mem-headroom-4x` | base × 4 | base × 1 | Larger sidecar headroom |
| `.mem-headroom-8x` | base × 8 | base × 1 | Maximum sidecar headroom (e.g. DuckDB) |

Examples:

* `medium.mem-4x` → pod 32 GB / Flink heap+managed ≈ 25.6 GB.
* `xlarge.mem-headroom-8x` → pod 256 GB / Flink heap+managed = 32 GB (baseline) / 224 GB headroom.

`.mem` is an alias for `.mem-2x`. The legacy `.mem-headroom` qualifier (triple memory, Flink stays at baseline) is **deprecated** — use `.mem-headroom-Nx` instead.

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
        "auto-expand-percentage": 0.2,  // Auto-expand threshold (0 to disable, must be < 1)
        "create-indexes": true,         // Whether to create table indexes (see "Create Indexes" below)
        "parameters": {}                // Extra postgresql.parameters (see "Parameters" below)
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

---

## Dedicated Nodes (`*-dedicated-nodes`)

Pins a component's pods onto dedicated nodes. Each engine's `deployment` accepts a list of dedicated-node names. Each name is a **hard requirement**: if no matching node is available, the pod stays `Pending` — it never falls back to a shared node.

| Engine | Field(s) |
| :--- | :--- |
| Flink | `taskmanager-dedicated-nodes`, `jobmanager-dedicated-nodes` |
| PostgreSQL | `dedicated-nodes` |
| Vert.x | `dedicated-nodes` |

```json5
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

| Engine | Field | Default |
| :--- | :--- | :--- |
| Flink | `do-not-disrupt` | `false` |
| PostgreSQL | `do-not-disrupt` | `true` |
| Vert.x | `do-not-disrupt` | `false` |

```json5
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

## Create Indexes (`create-indexes`)

Controls whether the PostgreSQL table indexes are created for the deployment. Defaults to `true`. PostgreSQL only.

| Engine | Field | Default |
| :--- | :--- | :--- |
| PostgreSQL | `create-indexes` | `true` |

Set it to `false` to bootstrap the database **tables-only**, skipping all index creation. This is intended for a catch-up profile that reprocesses a large backlog: writing to un-indexed tables drains the backlog faster. The indexes are then built when the deployment is upgraded back to a steady-state profile (where `create-indexes` returns to its `true` default), so a catch-up deployment must be followed by such an upgrade before it serves production query traffic.

```json5
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

## Parameters (`parameters`)

Extra PostgreSQL server parameters, merged into the database's `postgresql.parameters`. Any key here overrides the built-in default for that parameter. PostgreSQL only; empty by default.

| Engine | Field | Default |
| :--- | :--- | :--- |
| PostgreSQL | `parameters` | `{}` |

Typically used in a catch-up profile that trades durability for ingest throughput while reprocessing a large backlog — for example a larger `shared_buffers`/`max_wal_size` together with `synchronous_commit: off`. Set these only in the catch-up profile: on the steady-state upgrade any parameter not listed here reverts to its default.

```json5
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

