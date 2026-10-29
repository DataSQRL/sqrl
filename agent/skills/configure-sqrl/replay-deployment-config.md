# Replay Deployment Configuration

## What a replay is

A Flink job reads its sources, for example Kafka topics or Iceberg tables. When a new deployment starts, the sources can already hold months of old records. The job must process all of these old records before it can process the new records as they arrive. This first phase is the **replay**.

A replay processes much more data than normal operation. A **replay overlay** is a package file with settings that make the replay faster, for example more task managers and fewer checkpoints.

The team that operates the project deploys it in two phases:

1. The first deployment uses the replay overlay, and the job processes the old records.
2. When the job reaches the newest records, the team upgrades the same deployment without the replay overlay. The job keeps its state and continues with the normal settings. This is the **steady-state deployment**.

Your tasks are to create the replay overlay, [check it](#check-the-replay-overlay), and [write the deploy and upgrade steps](#deploy-and-upgrade) in the project README. The team does the deployment and the upgrade.

## When to create a replay overlay

Create `<sub-project>-replay-package.json` for every sub-project that has `flink` in `enabled-engines` and a `<sub-project>-prod-package.json`.
The team uses the same file again for each later deployment that starts with empty state.

## Layering

The replay overlay is the last file in the list, after the production configuration:

```bash
/opt/agent/cmd.sh compile -r . <project>-shared-package.json <sub-project>-prod-package.json <sub-project>-replay-package.json -b <sub-project>
```

Put only the values from [Values for the replay overlay](#values-for-the-replay-overlay) and, if needed, the [source start position](#source-start-position) in the file. All other settings come from the base and the production overlay. Leave `script`, `compiler` and `test-runner` out of the file.

## The rule: each operator keeps its id, type and state

At the upgrade, Flink takes a savepoint of the replay job. A savepoint is a saved copy of the state of each operator. The steady-state job then starts from that savepoint. Flink finds the state of each operator by the operator id in `deploy/plan/flink-compiled-plan.json`.

Before the upgrade, DataSQRL Cloud compares the replay plan with the steady-state plan. For each operator in the replay plan, it finds the operator with the same `id` in the steady-state plan. It refuses the upgrade when one of these is true:

- No operator in the steady-state plan has that `id`.
- The operator has a different `type`.
- The operator has a different `state`, `stateMetadata`, `joinSpec`, `inputProperties` or `inputChangelogMode` block.

DataSQRL Cloud ignores all other fields of an operator, for example `configuration` and the connector `options`. An operator that only the steady-state plan has is accepted.

Use this table to find out if a value can go in the replay overlay:

| Value | The compiler writes it to | Allowed in the replay overlay |
|---|---|---|
| `engines.<engine>.deployment.*` | `build/<sub-project>/package.json` | yes |
| `engines.flink.config` keys that start with `execution.`, `state.`, `pipeline.auto-watermark-interval`, `taskmanager.` | `deploy/plan/flink-config.yaml` | yes |
| `engines.flink.config` keys that start with `table.` | `deploy/plan/flink-config.yaml`, and also `deploy/plan/flink-compiled-plan.json` when an operator uses the key | only when the [check](#check-the-replay-overlay) passes |
| every `connectors.*` option | `deploy/plan/flink-compiled-plan.json` | only when the [check](#check-the-replay-overlay) passes |

When the replay and the steady state both need a value, put it in the base or the production overlay. Then the replay job and the steady-state job both use it.

## Values for the replay overlay

Start from these values, and size them for the number of old records. At the upgrade, each value goes back to its production value, or to its default.

| Setting | Replay value | Why |
|---|---|---|
| `engines.flink.deployment.taskmanager-count`, `taskmanager-size` | larger than production, for example twice the production count | More task managers process the old records faster. The parallelism of both deployments must divide `pipeline.max-parallelism` (see [Values that stay out](#values-that-stay-out-of-the-replay-overlay)). |
| `engines.flink.deployment.taskmanager-disk-size-gb` | larger than the default of the size, when the state is large | The RocksDB state grows fast during a replay. Kubernetes stops a task manager that uses more disk than it has. |
| `engines.flink.deployment.do-not-disrupt` | `true` | The autoscaler does not stop the long replay job to move it to a different machine. |
| `execution.checkpointing.interval` | `30 min` | Fewer checkpoints leave more time for processing. After a failure, the job repeats up to 30 minutes of work. |
| `execution.checkpointing.timeout` | `30 min` | During a replay the job runs at full load, so each checkpoint takes longer. |
| `execution.checkpointing.tolerable-failed-checkpoints` | `5` | One slow checkpoint does not stop the job. |
| `execution.checkpointing.unaligned.enabled` | `true` | Checkpoints can complete while the job runs at full load. |
| `execution.buffer-timeout.interval` | `500ms` | Flink sends larger network batches. This gives more throughput and adds up to 500 ms of delay. Nobody reads the results during the replay, so the delay has no cost. |
| `state.backend.rocksdb.writebuffer.size`, `writebuffer.count`, `thread.num` | `256mb`, `4`, `4` | Larger write buffers and more threads let RocksDB keep up with the fast state writes of the replay. |
| `engines.postgres.deployment.instance-size` | one size larger than production | The database receives all the writes of the replay. |
| `engines.postgres.deployment.create-indexes` | `false` | Writes to tables without indexes are faster. The upgrade builds the indexes. |
| `engines.postgres.deployment.parameters` | `shared_buffers` about a quarter of the instance memory, `max_wal_size: "32GB"`, `synchronous_commit: "off"` | These parameters make writes faster, but a database crash can lose the most recent writes. This is acceptable, because you can run a failed replay again. At the upgrade, each parameter that the steady-state configuration does not set goes back to its default. |
| `engines.postgres.deployment.replica-count` | `0` when production has replicas | Replicas copy every write of the replay, and they serve no traffic yet. |
| `engines.vertx.deployment.instance-count` | `1` | The API serves no traffic before the upgrade. |

## Values that stay out of the replay overlay

Put these values in the base or the production overlay:

- **`pipeline.max-parallelism`**: set it in the base. The savepoint stores this value, and Flink refuses to restore the savepoint with a parallelism that does not divide it. Use a value that both the replay parallelism and the steady-state parallelism divide, for example `128` with task manager counts that are powers of two.
- **`table.*` keys that add, remove or change operators**, for example `table.exec.mini-batch.*`, `table.optimizer.*` and `table.exec.sink.upsert-materialize`.
- **`engines.postgres.deployment.data-checksums`**: Postgres sets this value when it creates the database, and the value cannot change later.

## Source start position

The replay reads the old records only when the source starts at the oldest record. Set the start position in the production connector (see the `/manage-connector` skill) or in the replay overlay:

- For Kafka, use `scan.startup.mode: earliest-offset`.
- For Iceberg, use `starting-strategy: TABLE_SCAN_THEN_INCREMENTAL`.

The default `kafka-mutation` template sets `properties.auto.offset.reset: earliest`. The default `kafka` template sets no start position (see [connector-templates.md](connector-templates.md)). The default Iceberg `starting-strategy` is `INCREMENTAL_FROM_LATEST_SNAPSHOT`, so a new job reads only new snapshots and skips the old records (see [iceberg.md](../manage-connector/connectors/iceberg.md)).

After the upgrade, the job continues from the source position in its state, so the start position has no effect then.

## Example

`event_audit-replay-package.json`, after `event_audit-shared-package.json` and `event_audit-prod-package.json`:

```json
{
  "engines": {
    "flink": {
      "config": {
        "execution.checkpointing.interval": "30 min",
        "execution.checkpointing.timeout": "30 min",
        "execution.checkpointing.tolerable-failed-checkpoints": "5",
        "execution.checkpointing.unaligned.enabled": "true",
        "execution.buffer-timeout.interval": "500ms",
        "state.backend.rocksdb.writebuffer.size": "256mb",
        "state.backend.rocksdb.writebuffer.count": "4",
        "state.backend.rocksdb.thread.num": "4"
      },
      "deployment": {
        "taskmanager-size": "large.mem",
        "taskmanager-count": 4,
        "do-not-disrupt": true
      }
    },
    "postgres": {
      "deployment": {
        "instance-size": "large",
        "create-indexes": false,
        "parameters": {
          "shared_buffers": "4GB",
          "max_wal_size": "32GB",
          "synchronous_commit": "off"
        }
      }
    },
    "vertx": {
      "deployment": { "instance-count": 1 }
    }
  }
}
```

## Check the replay overlay

Run the same comparison as DataSQRL Cloud before the deployment:

1. Compile with the replay overlay, and keep a copy of the plan. This is the replay plan.
2. Compile without the replay overlay. This is the steady-state plan.
3. Compare the two plans with the script below.

```bash
/opt/agent/cmd.sh compile -r . <project>-shared-package.json <sub-project>-prod-package.json <sub-project>-replay-package.json -b <sub-project>
cp build/<sub-project>/deploy/plan/flink-compiled-plan.json /tmp/replay-plan.json
/opt/agent/cmd.sh compile -r . <project>-shared-package.json <sub-project>-prod-package.json -b <sub-project>
python3 - /tmp/replay-plan.json build/<sub-project>/deploy/plan/flink-compiled-plan.json <<'EOF'
import json, sys

STATE_FIELDS = ["state", "stateMetadata", "joinSpec", "inputProperties", "inputChangelogMode"]

def nodes_by_id(plan_path):
    with open(plan_path) as plan_file:
        return {str(node["id"]): node for node in json.load(plan_file)["nodes"] if "id" in node}

replay_nodes = nodes_by_id(sys.argv[1])
steady_nodes = nodes_by_id(sys.argv[2])
breaking_changes = []
for node_id, replay_node in replay_nodes.items():
    steady_node = steady_nodes.get(node_id)
    if steady_node is None:
        breaking_changes.append(f"operator id={node_id} removed")
        continue
    if replay_node.get("type") != steady_node.get("type"):
        breaking_changes.append(f"operator id={node_id} changed type {replay_node.get('type')} -> {steady_node.get('type')}")
    for field in STATE_FIELDS:
        if replay_node.get(field) != steady_node.get(field):
            breaking_changes.append(f"operator id={node_id} '{field}' block changed")
print("\n".join(breaking_changes) or "state-compatible")
sys.exit(1 if breaking_changes else 0)
EOF
```

The script prints `state-compatible` and exits 0 when DataSQRL Cloud accepts the upgrade. Otherwise it prints one line for each change and exits 1. Then find the value in the replay overlay that causes the change, for example a value from [Values that stay out](#values-that-stay-out-of-the-replay-overlay). Move it to the base or the production overlay when the steady state can use the same value, then run the check again.

## Deploy and upgrade

These steps are for the team that operates the project. Write them in the deployment section of the project README:

1. Deploy the sub-project with the production configuration and `<sub-project>-replay-package.json` as an extra package file, for example `--extra-package-json <sub-project>-replay-package.json`.
2. For a Kafka source, watch the Kafka consumer lag in DataSQRL Cloud. The lag is the number of records that the job did not read yet. Wait until the lag stops going down and stays small. For other sources, wait until the newest source records appear in the results.
3. Upgrade the same deployment in place with the production configuration only. The job starts from the savepoint, Postgres builds the indexes, and the sizes and parameters go back to their production values.
4. Send API traffic to the deployment only after the upgrade.
