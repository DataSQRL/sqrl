# Iceberg Connector

`'connector' = 'iceberg'`

Reads/writes data from Apache Iceberg tables. Supports both batch and streaming modes.

## Catalog Configuration

### Common Options

| Option | Default | Description |
|--------|---------|-------------|
| `type` | - | **Required**: Must be `iceberg` |
| `catalog-type` | `(none)` | `hive`, `hadoop`, `rest`, `glue`, `jdbc`, or `nessie` |
| `catalog-impl` | `(none)` | Fully-qualified class for custom catalog |
| `cache-enabled` | `true` | Enable catalog cache |
| `cache.expiration-interval-ms` | - | Cache TTL in ms (-1 disables expiration) |

### Hive Catalog

| Option | Description |
|--------|-------------|
| `uri` | **Required**: Hive metastore thrift URI |
| `warehouse` | Hive warehouse location |
| `clients` | Client pool size (default: 2) |
| `hive-conf-dir` | Path to directory with `hive-site.xml` |

### Hadoop Catalog

| Option | Description |
|--------|-------------|
| `warehouse` | **Required**: HDFS directory for metadata and data |

### REST Catalog

| Option | Description |
|--------|-------------|
| `uri` | **Required**: REST catalog URL |
| `credential` | OAuth2 client credentials |
| `token` | Bearer token for API access |

## Read Options

| Option | Default | Description |
|--------|---------|-------------|
| `streaming` | `false` | Enable streaming mode |
| `monitor-interval` | `60s` | Interval to discover new snapshots (streaming) |
| `starting-strategy` | `INCREMENTAL_FROM_LATEST_SNAPSHOT` | `TABLE_SCAN_THEN_INCREMENTAL`, `INCREMENTAL_FROM_LATEST_SNAPSHOT`, `INCREMENTAL_FROM_EARLIEST_SNAPSHOT`, `INCREMENTAL_FROM_SNAPSHOT_ID`, `INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP` |
| `snapshot-id` | `(none)` | Read from specific snapshot (batch time-travel) |
| `as-of-timestamp` | `(none)` | Read from snapshot at timestamp (batch time-travel) |
| `start-snapshot-id` | `(none)` | Start snapshot for incremental read |
| `start-snapshot-timestamp` | `(none)` | Start timestamp for incremental read |
| `end-snapshot-id` | latest | End snapshot for bounded reads |
| `branch` | `main` | Branch to read from |
| `tag` | `(none)` | Tag to read from |
| `split-size` | `128MB` | Target split size |
| `case-sensitive` | `false` | Case-sensitive column matching |
| `limit` | `-1` | Max rows to return (-1 unlimited) |
| `max-planning-snapshot-count` | unlimited | Max snapshots per split enumeration |
| `max-allowed-planning-failures` | `3` | Consecutive failures before job fails |

## Write Options

| Option | Default | Description |
|--------|---------|-------------|
| `write-format` | table default | `parquet`, `avro`, or `orc` |
| `target-file-size-bytes` | table default | Target file size |
| `upsert-enabled` | table default | Enable upsert mode (requires equality fields) |
| `overwrite-enabled` | `false` | Overwrite existing data |
| `distribution-mode` | table default | `none`, `hash`, or `range` |
| `compression-codec` | table default | Compression codec |
| `compression-level` | table default | Compression level (Parquet/Avro) |
| `write-parallelism` | upstream | Writer parallelism |

## Key Notes

- **Streaming source**: Set `streaming=true` and `monitor-interval` to continuously read new snapshots
- **Upsert mode**: Requires table with identifier/equality fields defined; performs delete+insert
- **Time-travel**: Use `snapshot-id` or `as-of-timestamp` for point-in-time queries
- **SQL hints**: Pass options via `/*+ OPTIONS('option'='value') */` in Flink SQL
- **Catalog creation**: Create catalog first with `CREATE CATALOG`, then use `catalog.database.table` references

[Reference Documentation](https://iceberg.apache.org/docs/nightly/flink-configuration/)
