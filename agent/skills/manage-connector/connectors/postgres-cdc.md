# PostgreSQL CDC Connector

`'connector' = 'postgres-cdc'`

Reads an initial snapshot and subsequent logical-replication changes from PostgreSQL. The connector is included in the SQRL CDC runtime.

## Configuration Options

### Required Options

| Option | Description |
|---|---|
| `hostname` | PostgreSQL server hostname or IP address. |
| `username` | Database user with logical-replication access. |
| `password` | Database-user password. |
| `database-name` | Database to monitor. |
| `schema-name` | Schema name or regular expression of schemas to monitor. |
| `table-name` | Table name or regular expression of tables to monitor. |
| `slot.name` | Logical replication slot used to stream changes. |

### Important Optional Options

| Option | Default | Description |
|---|---|---|
| `port` | `5432` | PostgreSQL server port. |
| `decoding.plugin.name` | `decoderbufs` | Installed logical-decoding plugin; supported values include `decoderbufs`, `wal2json`, and `pgoutput`. |
| `changelog-mode` | `all` | `all` emits a retraction stream; `upsert` requires a primary key and emits idempotent key updates. |
| `heartbeat.interval.ms` | `30 s` | Heartbeat interval used to advance the latest available replication-slot offset. |
| `scan.incremental.snapshot.enabled` | `false` | Enables parallel, chunked snapshots with checkpointing. |
| `scan.startup.mode` | `initial` | Startup position: `initial`, `latest-offset`, `committed-offset`, or `snapshot`. Available when incremental snapshots are enabled. |
| `scan.incremental.close-idle-reader.enabled` | `false` | Close idle readers after the snapshot phase. |
| `scan.lsn-commit.checkpoints-num-delay` | `3` | Number of checkpoints to delay before committing LSN offsets; applies when incremental snapshots are enabled. |
| `scan.incremental.snapshot.chunk.key-column` | — | Snapshot chunk key; the first primary-key column is used when it is not set. Required and non-null for a table without a primary key. |
| `scan.snapshot.fetch.size` | `1024` | Rows fetched per snapshot poll. |
| `scan.read-changelog-as-append-only.enabled` | `false` | Convert all change events to inserts; use only with the `row_kind` metadata field for logical-delete handling. |
| `scan.include-partitioned-tables.enabled` | `false` | Read partitioned tables through their root; requires a publication created with `publish_via_partition_root=true`. |
| `scan.incremental.snapshot.backfill.skip` | `false` | Skip snapshot backfill. This can replay changes and provides at-least-once rather than exactly-once semantics. |
| `debezium.*` | — | Pass-through Debezium PostgreSQL properties. |

## Prerequisites

- Configure PostgreSQL for logical replication and ensure the selected decoding plugin is installed.
- Create or allow the connector to use the configured replication slot; slot names may contain lowercase letters, numbers, and underscores.
- Use a distinct `slot.name` for each captured table to avoid replication-slot conflicts such as an active `flink` slot.
- Grant the CDC user the permissions required to create snapshots and consume logical replication for the selected database and tables.

## Key Notes

- `upsert` changelog mode is suitable only when a primary key exists and the downstream consumer expects key-based updates.
- For a table without a primary key, use a stable non-null chunk key. Updating that key during the snapshot can reduce delivery guarantees to at-least-once; use an idempotent downstream sink.
- If incremental snapshots remain disabled, large snapshots cannot create recoverable checkpoints while they are being scanned. Enable incremental snapshots or configure checkpoint timeouts accordingly.

[Reference Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/postgres-cdc/)
