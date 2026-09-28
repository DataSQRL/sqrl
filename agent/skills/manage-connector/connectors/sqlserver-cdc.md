# SQLServer CDC Connector

`'connector' = 'sqlserver-cdc'`

Reads CDC change streams from Microsoft SQL Server databases. The connector is included in the SQRL CDC runtime.

## Configuration Options

### Required Options

| Option | Description |
|--------|-------------|
| `hostname` | SQL Server IP or hostname |
| `username` | Database user |
| `password` | Database password |
| `database-name` | Database to monitor |
| `table-name` | Table in format `schema.table` |

### Optional Options

| Option | Default | Description |
|--------|---------|-------------|
| `port` | `1433` | SQL Server port |
| `server-time-zone` | `UTC` | Session timezone (e.g., `'Asia/Shanghai'`) |
| `scan.incremental.snapshot.enabled` | `true` | Enable parallel snapshot reading |
| `scan.startup.mode` | `initial` | `initial` snapshots schema and data; `latest-offset` snapshots schema only and then reads new changes. Do not combine it with `debezium.snapshot.mode`. |
| `scan.incremental.snapshot.chunk.key-column` | - | Column for snapshot partitioning (**required for tables without primary keys**) |
| `scan.incremental.close-idle-reader.enabled` | `false` | Close idle readers after snapshot |
| `scan.incremental.snapshot.unbounded-chunk-first.enabled` | `true` | Assign unbounded chunks first to reduce out-of-memory risk during snapshotting. |
| `scan.incremental.snapshot.backfill.skip` | `false` | Skip backfill (risks data inconsistency) |
| `chunk-meta.group.size` | `1000` | Snapshot metadata chunk size |
| `debezium.*` | - | Passthrough properties for Debezium engine |

## Prerequisites

**CDC must be enabled** on both database and table before using this connector:

1. SQL Server Agent must be running
2. CDC enabled on database
3. User must have `db_owner` role
4. Execute `sys.sp_cdc_enable_table` for each monitored table

## Key Notes

- **Single-threaded change reading**: Multiple readers during snapshot, but change events consumed sequentially by one task
- **Checkpoint during snapshot**: Cannot create recoverable checkpoints while scanning snapshots. Configure extended checkpoint timeouts and tolerate failures during snapshot phase
- **Non-PK chunk columns**: Using non-primary key as `chunk.key-column` can cause data inconsistency if that column is updated during snapshot
- **At-least-once during updates**: If chunk key columns are updated during snapshot, only at-least-once semantics guaranteed. Implement idempotent downstream operations

[Reference Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/sqlserver-cdc/)
