# Flink Engine Configuration

| Key          | Type       | Default   | Notes                                                                                              |
|--------------|------------|-----------|----------------------------------------------------------------------------------------------------| 
| `config`     | **object** | see below | Copied verbatim into the generated Flink SQL job (e.g. `"table.exec.source.idle-timeout": "5 s"`). |

```json5
{
  "engines": {
    "flink": {
      "config": {
        "execution.runtime-mode": "STREAMING", //or "BATCH" for batch pipelines
        "table.exec.source.idle-timeout": "30 sec" // so pipeline can advance when some sources are idle
      }
    }
  }
}
```

In STREAMING mode, DataSQRL sets `execution.checkpointing.interval: 30 s`, `execution.checkpointing.min-pause: 20 s`, and `table.exec.source.idle-timeout: 1 s` unless the package configuration sets them explicitly.

Frequently configured options:
* `execution.runtime-mode`: `BATCH` or `STREAMING` which controls how the pipeline is executed.
* `taskmanager.memory.network.max`: How much memory to assign to the network buffers.


## Common Flink Table Settings

| Configuration Key | Default | Common Values / Range | How to Configure                                                                                              |
|---|---|---|---------------------------------------------------------------------------------------------------------------|
| `table.exec.mini-batch.enabled` | `false` | `true` | Enables mini-batch optimization to reduce state access overhead in aggregations and more efficient processing |
| `table.exec.mini-batch.allow-latency` | `0 ms` | `1s` – `5s` | Max wait time to accumulate a mini-batch; trade-off between latency and throughput. >0 when enabled.          |
| `table.exec.mini-batch.size` | `-1` | `1000` – `10000` | Max number of records per mini-batch bundle. >0 when enabled                                                  |
| `table.exec.state.ttl` | `0 ms` (no expiry) | `1d`, `7d`, `24h` | Caps state size for unbounded joins and aggregations to prevent OOM                                           |
| `table.optimizer.agg-phase-strategy` | `AUTO` | `TWO_PHASE`, `ONE_PHASE` | Forces two-phase (partial + final) aggregation for better parallelism                                         |
| `table.optimizer.distinct-agg.split.enabled` | `false` | `true` | Splits `COUNT(DISTINCT ...)` into two phases to avoid data skew                                               |
| `table.optimizer.join-reorder-enabled` | `false` | `true` | Lets the optimizer reorder joins based on table statistics for better plans                                   |
| `table.exec.source.idle-timeout` | `1 s` in STREAMING mode (set by DataSQRL); `0 s` (disabled) for `test` | `30s`, `60s` | Marks idle sources so watermarks can advance when some partitions go quiet                                    |
| `table.exec.sink.not-null-enforcer` | `ERROR` | `DROP` | Controls whether null-constraint violations throw errors or silently drop rows                                |
| `table.exec.sink.upsert-materialize` | `AUTO` | `NONE`, `FORCE` | Controls materialization of upsert streams before writing to non-upsert sinks                                 |
| `table.optimizer.reuse-sub-plan-enabled` | `true` | `false` | Disabling can help when shared sub-plans cause unexpected state sharing                                       |
| `table.optimizer.reuse-source-enabled` | `true` | `false` | Controls whether identical source scans are shared across the plan                                            |
| `table.exec.async-lookup.buffer-capacity` | `100` | `10` – `1000` | Max number of in-flight async lookup requests for async lookup joins                                          |
| `table.exec.async-lookup.timeout` | `3 min` | `30s`, `1min` | Timeout for each async lookup request before it fails                                                         |
| `table.exec.resource.default-parallelism` | `-1` (inherit) | `4`, `8`, `16` | Sets operator-level default parallelism for table/SQL jobs                                                    |
| `table.dynamic-table-options.enabled` | `true` | `false` | Allows per-query connector option overrides via SQL hints (`/*+ OPTIONS(...) */`)                             |
| `table.optimizer.multiple-input-enabled` | `true` | `false` | Controls chaining of multiple operators into a single task for reduced overhead                               |
| `table.exec.rank.topn-cache-size` | `10000` | `1000` – `100000` | Cache size for TopN operator; larger cache reduces state reads but uses more heap                             |
| `table.optimizer.bushy-join-reorder-threshold` | `12` | `4` – `20` | Max number of joins considered for bushy tree reordering                                                      |
| `table.exec.window-agg.buffer-size-limit` | `100000` | `10000` – `500000` | Row buffer limit for window aggregation before spilling; affects memory vs. speed                             |

## Common Flink Execution Settings

| Configuration Key | Default | Common Values / Range | How to Configure |
|---|---|---|---|
| `execution.runtime-mode` | `STREAMING` | `BATCH` | Switched for bounded DataStream batch jobs |
| `execution.checkpointing.interval` | `30 s` in STREAMING mode (set by DataSQRL) | `1min`, `5min` | Checkpoint frequency; trades recovery time and end-to-end latency of transactional sinks against checkpoint overhead |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | `AT_LEAST_ONCE` | Relaxed for higher throughput when exactly-once isn't required |
| `execution.checkpointing.timeout` | `10min` | `2min` – `30min` | Increased when checkpoints are slow due to large state or slow storage |
| `execution.checkpointing.unaligned.enabled` | `false` | `true` | Enabled to speed up checkpointing under heavy backpressure |
| `execution.checkpointing.min-pause` | `20 s` in STREAMING mode (set by DataSQRL) | `10s` – `1min` | Prevents checkpoint storms by enforcing a gap between checkpoints |
| `execution.checkpointing.max-concurrent-checkpoints` | `1` | `1` | Rarely increased; relevant when unaligned checkpoints are disabled |
| `execution.checkpointing.tolerable-failed-checkpoints` | `0` | `3` – `5` | Prevents job failure from transient checkpoint issues |
| `taskmanager.memory.process.size` | (none) | `2gb` – `16gb` | Primary memory sizing knob for containerized deployments |
| `taskmanager.memory.managed.fraction` | `0.4` | `0.3` – `0.6` | Adjusted when RocksDB needs more or less managed memory |
| `taskmanager.memory.network.fraction` | `0.1` | `0.05` – `0.2` | Increased for jobs with high shuffle/network traffic |
| `taskmanager.memory.network.max` | `infinite`; `800m` for `run` | `1gb`, `2gb`, `4gb` | Upper bound on network memory; set explicitly to cap buffer memory usage |
| `jobmanager.memory.process.size` | (none) | `1gb` – `4gb` | Required sizing for containerized JobManager deployments |
| `parallelism.default` | `1` | `4` – `256` | Sets job-wide default parallelism |
| `pipeline.auto-watermark-interval` | `200ms` | `500ms`, `1s`, `5s` | Increased to reduce watermark overhead in high-throughput jobs |
| `pipeline.max-parallelism` | `-1` | `128` – `4096` | Set explicitly to control key-group count and future rescaling |
| `state.backend.rocksdb.writebuffer.size` | `64mb` | `128mb` – `256mb` | Increased for write-heavy workloads to reduce flush frequency |
| `state.backend.rocksdb.writebuffer.count` | `2` | `4` – `8` | Increased alongside writebuffer.size for write throughput |
| `state.backend.rocksdb.thread.num` | `2` | `4` – `8` | Increased to parallelize RocksDB compaction and flush |
| `state.backend.rocksdb.checkpoint.transfer.thread.num` | `4` | `8` – `16` | Increased to speed up checkpoint upload/download |
| `execution.buffer-timeout.interval` | `100ms` | `0`, `200ms`, `500ms` | Lowered for latency, raised or disabled for throughput |
| `execution.buffer-timeout.enabled` | `true` | `false` | Disabled to maximize throughput at cost of latency |
| `taskmanager.network.memory.buffer-debloat.enabled` | `false` | `true` | Enabled to automatically tune in-flight buffer sizes |
| `heartbeat.timeout` | `50s` | `30s` – `120s` | Increased in flaky network or cloud environments |
| `heartbeat.interval` | `10s` | `5s` – `30s` | Tuned alongside timeout for failure detection sensitivity |
| `pekko.ask.timeout` | `10s` | `30s` – `60s` | Increased when RPC timeouts occur under load |
| `pekko.framesize` | `10mb` | `50mb` – `200mb` | Increased when large serialized objects are passed via RPC |
| `execution.checkpointing.aligned-checkpoint-timeout` | `0ms` | `30s` – `5min` | Set when using unaligned checkpoints with fallback to aligned |
| `execution.state-recovery.ignore-unclaimed-state` | `false` | `true` | Set only for one restore when the check marks a line with `[unclaimed state]`; drops that state (see below) |

## Upgrading from a Savepoint

Flink refuses to restore a savepoint that holds state for an operator the new `flink-compiled-plan.json` no longer has.

Before an upgrade, check that the new plan can restore the state of the running version. Compile the new version with `--check-state-upgrade-from` and the build folder of the running version:

```bash
/opt/agent/cmd.sh compile -r myproject myproject-shared-package.json myproject-prod-package.json -b myproject --check-state-upgrade-from build/myproject-deployed
```

- Use the build folder of the running version as the old build. A new compile of an older script can give a different plan than the plan that runs. You can also compile the package files of the running version with their own `-b` folder first. Do this only when the running version has the same script and DataSQRL version.
- Give each compile its own `-b` folder. Each compile empties its build folder, and the default build folder `build` holds every other build folder.
- Run the check again after each change to the script or to the package files.
- The compile fails with one line for each break, for example `operator id=7 changed: field 'state' differs: <old value> -> <new value> (<operator description>)`. When the check reports a break, the new build is still in the target folder. Do not deploy that build, and check the exit code of the compile. Find the change in the script or the package files that causes the break. Fix the change. You can accept a break only when its line has the mark `[unclaimed state]`, as described below. A line with the mark `[incompatible state]` cannot be accepted: its operator claims the old state but cannot read it, and `execution.state-recovery.ignore-unclaimed-state` does not help. Change the script back, or start the new job without the savepoint. A line without a mark belongs to an operator without state, for example a `calc` or an `exchange`. Such a line can still change the state of the next operator. Example: a changed key on an `exchange` sends each row to the state of another key. When the old and the new key have the same type, Flink restores without an error, but the results are wrong.
- The operator ids follow the plan order. A new or removed operator moves the ids of each operator after it, so one change can give many lines. Start at the first id with a break, because the cause is usually there. A moved operator with state loses its state, because Flink gives the old state to the operator at the old id. Its lines get the mark `[unclaimed state]`.
- When only one build sets `pipeline.max-parallelism`, the compile gives a warning.
  - Only the old build sets it: Flink takes the value from the savepoint. Make sure that no other configuration gives the new job a different value.
  - Only the new build sets it: make sure that the running job uses the same value.
- A new column, a removed column, a changed type or a changed `NOT NULL` on a table that an operator with state stores is a break, for example on a `DISTINCT` table, a `GROUP BY` table, a `GROUP BY` table with a `TUMBLE` window, or a join. One of the lines names the field `outputType`. A renamed column is not a break, and neither is a new column on a `TUMBLE`, `HOP` or `CUMULATE` window table without an aggregate, because that window keeps no state.
- A changed aggregate function, for example `SUM` changed to `MAX`, is a break, because the stored value of the old function does not fit the new function. One of the lines names the field `aggCalls`. `SUM` and `SUM0` count as the same function.
- A change of `table.exec.state.ttl` is not a break. The compile gives a notice for each state whose TTL changes. You do not need to change the TTL back.
- A pass means that Flink can restore the state. It does not mean that the results are the same as the results of a fresh run. Tell the user about each of these cases:
  - A new table at the end of the plan starts with empty state, and it sees only the records after the upgrade. To include the older records, start the new job without the savepoint, when the source still has them.
  - A new table whose operators come before the old operators can take the state of old operators of the same kind, and the check passes. This occurs for a source of the same connector, a deduplicate with the same column types, and an aggregate with the same key type and aggregate functions. After you add a table, compare the table name of each source with the same id in the old and the new `flink-compiled-plan.json`. A different table name at the same id means that the new table takes the old state.
  - A changed key with the same type, for example a deduplicate on `customer` in place of `id`, restores without an error, but the old state belongs to the wrong keys.
  - A changed input column of the same type in an aggregate, for example `SUM(other)` in place of `SUM(val)`, restores without an error, but the new sum continues from the old sum.

The mark `[unclaimed state]` means that no operator of the new plan claims the old state of that line. Flink matches the state of an operator by its id and its kind. A removed operator with state, an operator with state whose kind changes at the same id, and a removed `state` entry leave such state. A source or a sink can also hold state that the plan does not show, so a moved source or sink also gets the mark. A new table, or a new column that adds a step to the plan, moves the operator ids and also gives such lines. To accept these lines, restore once with `execution.state-recovery.ignore-unclaimed-state: true`. With the Flink Kubernetes Operator, use `job.allowNonRestoredState: true`. Then remove the option, so that it cannot hide an unexpected state mismatch later.
The state of the removed operators is dropped. For example, plans compiled by earlier DataSQRL versions write `STREAM` tables through `WatermarkTimestampAssigner` and `SinkMaterializer` operators that current plans no longer have; rows still held in their state are not written to the database after the restore.
