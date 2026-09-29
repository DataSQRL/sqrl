# Upsert Kafka Connector

`'connector' = 'upsert-kafka'`

Reads/writes changelog streams from Kafka using upsert semantics. Records with identical keys are treated as updates; null values represent deletions.

## Configuration Options

### Required Options

| Option | Description |
|--------|-------------|
| `topic` | Kafka topic name (multiple topics separated by semicolons) |
| `properties.bootstrap.servers` | Comma-separated Kafka broker addresses |
| `key.format` | Format for message keys |
| `value.format` | Format for message values |

### Optional Options

| Option | Default | Description |
|--------|---------|-------------|
| `properties.*` | - | Passthrough Kafka client properties |
| `key.fields-prefix` | `(none)` | Prefix for key format fields to avoid naming conflicts |
| `value.fields-include` | `ALL` | `ALL` or `EXCEPT_KEY` - whether to include key columns in value |
| `scan.parallelism` | upstream | Source parallelism |
| `sink.parallelism` | upstream | Sink parallelism |
| `sink.buffer-flush.max-rows` | `0` | Max buffered records before flush (0 = disabled) |
| `sink.buffer-flush.interval` | `0` | Flush interval for async writes (0 = disabled) |
| `sink.delivery-guarantee` | `at-least-once` | `none`, `at-least-once`, or `exactly-once` |
| `sink.transactional-id-prefix` | `(none)` | **Required for exactly-once** - transaction ID prefix |

## Key Notes

- **Primary key required**: Table DDL must define a `PRIMARY KEY` constraint
- **Partitioning by key**: Data partitioned by primary key values to maintain ordering per key
- **Exactly-once requirements**: Requires checkpointing enabled AND `sink.transactional-id-prefix` configured
- **Delete representation**: Kafka messages with null values are interpreted as deletes

[Reference Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/upsert-kafka/)
