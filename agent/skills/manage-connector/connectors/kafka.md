# Kafka Connector

`'connector' = 'kafka'`

Reads/writes append-only streams from Apache Kafka topics.

## Configuration Options

### Required Options

| Option | Description |
|--------|-------------|
| `topic` | Topic name (semicolon-separated list for source, single topic for sink) |
| `properties.bootstrap.servers` | Comma-separated Kafka broker addresses |
| `format` or `value.format` | Format for message values |

### Source Options

| Option | Default | Description |
|--------|---------|-------------|
| `topic-pattern` | `(none)` | Regex for dynamic topic discovery (alternative to `topic`) |
| `properties.group.id` | auto-generated | Consumer group ID |
| `scan.startup.mode` | `group-offsets` | `earliest-offset`, `latest-offset`, `group-offsets`, `timestamp`, `specific-offsets` |
| `scan.startup.timestamp-millis` | `(none)` | Epoch millis for `timestamp` mode |
| `scan.startup.specific-offsets` | `(none)` | Format: `'partition:0,offset:42;partition:1,offset:300'` |
| `scan.bounded.mode` | unbounded | End position: `latest-offset`, `group-offsets`, `timestamp`, `specific-offsets` |
| `scan.topic-partition-discovery.interval` | `5 min` | Interval for new partition discovery |
| `scan.parallelism` | upstream | Source parallelism |

### Sink Options

| Option | Default | Description |
|--------|---------|-------------|
| `sink.partitioner` | `default` | `default`, `fixed`, `round-robin`, or custom class |
| `sink.delivery-guarantee` | `at-least-once` | `none`, `at-least-once`, or `exactly-once` |
| `sink.transactional-id-prefix` | `(none)` | **Required for exactly-once** |
| `sink.parallelism` | upstream | Sink parallelism |

### Key Options

| Option | Default | Description |
|--------|---------|-------------|
| `key.format` | `(none)` | Format for message keys (optional) |
| `key.fields` | `(none)` | Semicolon-separated physical columns composing the key |
| `key.fields-prefix` | `(none)` | Prefix for key format fields to avoid naming conflicts |
| `value.fields-include` | `ALL` | `ALL` or `EXCEPT_KEY` |

## Available Metadata

| Key | Type | R/W | Description |
|-----|------|-----|-------------|
| `topic` | `STRING NOT NULL` | R | Topic name |
| `partition` | `INT NOT NULL` | R | Partition ID |
| `offset` | `BIGINT NOT NULL` | R | Message offset |
| `timestamp` | `TIMESTAMP_LTZ(3) NOT NULL` | R/W | Message timestamp |
| `timestamp-type` | `STRING NOT NULL` | R | `NoTimestampType`, `CreateTime`, or `LogAppendTime` |
| `headers` | `MAP<STRING, BYTES> NOT NULL` | R/W | Message headers |
| `leader-epoch` | `INT NULL` | R | Leader epoch (if available) |

## Key Notes

- **Idle partitions block watermarks**: Use `'table.exec.source.idle-timeout'` to advance watermarks when partitions have no data
- **Per-partition watermarks**: Watermarks generated per partition, merged downstream
- **Exactly-once requirements**: Requires checkpointing + `sink.transactional-id-prefix`; consumers need appropriate `isolation.level`
- **Sink topic**: Only single topic supported for sink (no semicolon-separated list)

[Reference Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/kafka/)
