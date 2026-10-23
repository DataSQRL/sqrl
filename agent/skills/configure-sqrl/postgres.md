# PostgreSQL Engine Configuration

Postgres needs no mandatory engine configuration: DataSQRL generates the physical DDL for tables, indexes, and views. It does support the following settings for TTL-partitioned tables.

| Key                     | Type        | Default | Description                                                     |
|-------------------------|-------------|--------:|-----------------------------------------------------------------|
| `partition-ttl-divisor` | **integer** |   `100` | Divides a table TTL to cap the number of pg_partman partitions. |
| `partition-premake`     | **integer** |     `4` | Number of future partitions that pg_partman creates in advance. |

Tables annotated with a timestamp `partition_key` and a `ttl` hint use range partitions. The TTL unit establishes the smallest interval and the derived width is snapped down to a calendar-aligned interval. For example, with the default divisor, `ttl(14 days)` creates daily partitions. Retention-window partitions are created at setup time, so replayed or late events can land in the correct dated partition.

By default, PostgreSQL inserts rows of `STREAM` tables with `ON CONFLICT (...) DO NOTHING`, `STATE` tables with `ON CONFLICT (...) DO UPDATE` guarded by their timestamp column, and other tables with `ON CONFLICT (...) DO UPDATE`. Setting `sink.on-conflict.action` (`UPDATE`, `TIMESTAMP`, or `IGNORE`) in the `postgres` connector template overrides this for every table; `TIMESTAMP` also needs `sink.on-conflict.timestamp-column`. The primary key of a `STREAM` table (a hash of all columns if it has none) is expected, but not enforced, to identify each row, so a conflict is normally a replay of the same row. If rows with the same key differ, rows in one write batch are first reduced to the last one, and the conflict action then decides against the stored row; `DO NOTHING` keeps the stored row.

The connector templates control how Flink writes to and the server reads from Postgres. The defaults work for most cases; read [connector templates](connector-templates.md) before changing them. For cloud instance sizing, replicas, disks, or PostgreSQL server parameters, read [cloud deployment](cloud-deployment.md).
