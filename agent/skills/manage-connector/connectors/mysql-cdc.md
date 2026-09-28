# MySQL CDC Connector

`'connector' = 'mysql-cdc'`

Reads an initial snapshot and subsequent binlog changes from MySQL-compatible databases. The connector is included in the SQRL CDC runtime.

## Configuration Options

### Required Options

| Option | Description |
|---|---|
| `hostname` | MySQL server hostname or IP address. |
| `username` | Database user with CDC privileges. |
| `password` | Database-user password. |
| `database-name` | Database name or regular expression of databases to monitor. |
| `table-name` | Table name or regular expression of tables to monitor. |

### Important Optional Options

| Option | Default | Description |
|---|---|---|
| `port` | `3306` | MySQL server port. |
| `server-id` | Random value in `5400-6400` | Unique numeric ID, or an ID range for parallel snapshot readers. Set an explicit, non-overlapping value/range for each running CDC job. |
| `scan.incremental.snapshot.enabled` | `true` | Enables parallel, chunked snapshots with checkpointing; use an ID range larger than source parallelism when it is enabled. |
| `scan.incremental.close-idle-reader.enabled` | `false` | Close idle readers after the snapshot phase. |
| `scan.incremental.snapshot.chunk.size` | `8096` | Rows per snapshot chunk. |
| `scan.snapshot.fetch.size` | `1024` | Rows fetched per snapshot poll. |
| `scan.incremental.snapshot.chunk.key-column` | — | Snapshot chunk key; the first primary-key column is used when it is not set. A non-primary-key choice can reduce consistency and performance. |
| `scan.startup.mode` | `initial` | `initial`, `earliest-offset`, `latest-offset`, `specific-offset`, `timestamp`, or `snapshot`. |
| `server-time-zone` | JVM default zone | Database session time zone used to interpret MySQL `TIMESTAMP` values. |
| `connect.timeout` / `connect.max-retries` | `30 s` / `3` | Connection timeout and retry limit. |
| `connection.pool.size` | `20` | Connection-pool size. |
| `jdbc.properties.*` | — | Additional JDBC URL properties, such as `jdbc.properties.useSSL`. |
| `heartbeat.interval` | `30 s` | Binlog-position heartbeat interval; set to `0 s` only when intentionally disabling it. |
| `scan.incremental.snapshot.backfill.skip` | `false` | Skip snapshot backfill. This can replay changes and provides at-least-once rather than exactly-once semantics. |
| `debezium.*` | — | Pass-through Debezium MySQL properties. |

## Prerequisites

- Enable binary logging with row-based events and retain binlogs long enough for restart recovery.
- Grant the CDC user `SELECT`, `SHOW DATABASES`, `REPLICATION SLAVE`, and `REPLICATION CLIENT` privileges. `RELOAD` is not required when incremental snapshots are enabled.
- Choose a unique `server-id` or range across concurrently running CDC readers; duplicate IDs can cause readers to use the wrong binlog position.

## Key Notes

- `database-name` and `table-name` are combined into a fully qualified regular expression, so escape regex metacharacters when matching literal names.
- A primary key is strongly preferred. For a table without one, choose a stable, non-null chunk key and verify the resulting update semantics.
- Long snapshots can exceed MySQL connection timeouts; configure `interactive_timeout` and `wait_timeout` appropriately.

[Reference Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.6/docs/connectors/flink-sources/mysql-cdc/)
