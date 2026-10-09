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
| `table.exec.sink.upsert-materialize` | `AUTO` | `NONE`, `FORCED` | Controls materialization of upsert streams before writing to non-upsert sinks                                 |
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