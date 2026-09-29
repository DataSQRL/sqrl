# FileSystem Connector

`'connector' = 'filesystem'`

Read/write partitioned files from local or distributed filesystems. Requires format specification.

## Configuration Options

### Basic Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `path` | **yes** | - | Directory path (not file path) |
| `format` | **yes** | - | File format: csv, json, avro, parquet, orc, etc. |
| `partition.default-name` | no | - | Default partition name for null/empty values |
| `source.path.regex-pattern` | no | - | Regex to filter files (matches absolute path) |

### Source Options

| Option | Default | Description |
|--------|---------|-------------|
| `source.monitor-interval` | `(none)` | Interval to check for new files. If not set, source is **bounded** (scans once). Set for streaming (e.g., `'10s'`) |

### Sink Rolling Policy

| Option | Default | Description |
|--------|---------|-------------|
| `sink.rolling-policy.file-size` | `128MB` | Max part file size before rolling |
| `sink.rolling-policy.rollover-interval` | `30 min` | Max time a part file stays open |
| `sink.rolling-policy.check-interval` | `1 min` | Frequency to check time-based rolling |

### File Compaction

| Option | Default | Description |
|--------|---------|-------------|
| `auto-compaction` | `false` | Merge small files after checkpoint |
| `compaction.file-size` | rolling size | Target compacted file size |

### Partition Commit Trigger

| Option | Default | Description |
|--------|---------|-------------|
| `sink.partition-commit.trigger` | `process-time` | `process-time` or `partition-time` (requires watermark) |
| `sink.partition-commit.delay` | `0 s` | Delay before commit (e.g., `'1 h'` for hourly partitions) |
| `sink.partition-commit.watermark-time-zone` | `UTC` | Time zone for partition-time trigger. **Must match session time zone if watermark on TIMESTAMP_LTZ** |

### Partition Time Extractor

| Option | Default | Description |
|--------|---------|-------------|
| `partition.time-extractor.kind` | `default` | `default` or `custom` |
| `partition.time-extractor.timestamp-pattern` | - | Pattern like `'$year-$month-$day $hour:00:00'` or `'$dt'` |
| `partition.time-extractor.timestamp-formatter` | `yyyy-MM-dd HH:mm:ss` | Format for parsing the timestamp pattern |
| `partition.time-extractor.class` | - | Custom `PartitionTimeExtractor` implementation |

### Partition Commit Policy

| Option | Default | Description |
|--------|---------|-------------|
| `sink.partition-commit.policy.kind` | `(none)` | `success-file`, `metastore`, or both: `'metastore,success-file'` |
| `sink.partition-commit.success-file.name` | `_SUCCESS` | Success file name |
| `sink.partition-commit.policy.class` | - | Custom `PartitionCommitPolicy` implementation |

### Other Sink Options

| Option | Default | Description |
|--------|---------|-------------|
| `sink.parallelism` | upstream | Write parallelism (**INSERT-ONLY changelog only**) |
| `sink.shuffle-by-partition.enable` | `false` | Shuffle by partition fields (reduces files but may cause skew) |

## Available Metadata

| Key | Type | Description |
|-----|------|-------------|
| `file.path` | `STRING NOT NULL` | Full file path |
| `file.name` | `STRING NOT NULL` | File name only |
| `file.size` | `BIGINT NOT NULL` | File size in bytes |
| `file.modification-time` | `TIMESTAMP_LTZ(3) NOT NULL` | File modification time |

## Key Notes

- **Path is directory**: The connector reads/writes to a directory, not individual files
- **Bounded by default**: Set `source.monitor-interval` for streaming/continuous reading
- **Hive-style partitioning**: Auto-discovers partitions from directory structure (e.g., `datetime=2019-08-25/hour=11/`)
- **No ingestion order**: Files in directory are read in undefined order
- **Bulk vs row formats**: Parquet/ORC/Avro (bulk) finalize on checkpoint; CSV/JSON (row) respect rolling policy
- **Compaction visibility**: Compacted files invisible until compaction completes (checkpoint interval + compaction time)
- **TIMESTAMP_LTZ watermarks**: Must set `sink.partition-commit.watermark-time-zone` to session time zone, otherwise partition commit may delay hours
