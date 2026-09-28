# PostgreSQL Engine Configuration

Postgres needs no mandatory engine configuration: DataSQRL generates the physical DDL for tables, indexes, and views. It does support the following settings for TTL-partitioned tables.

| Key                     | Type        | Default | Description                                                     |
|-------------------------|-------------|--------:|-----------------------------------------------------------------|
| `partition-ttl-divisor` | **integer** |   `100` | Divides a table TTL to cap the number of pg_partman partitions. |
| `partition-premake`     | **integer** |     `4` | Number of future partitions that pg_partman creates in advance. |

Tables annotated with a timestamp `partition_key` and a `ttl` hint use range partitions. The TTL unit establishes the smallest interval and the derived width is snapped down to a calendar-aligned interval. For example, with the default divisor, `ttl(14 days)` creates daily partitions. Retention-window partitions are created at setup time, so replayed or late events can land in the correct dated partition.

The connector templates control how Flink writes to and the server reads from Postgres. The defaults work for most cases; read [connector templates](connector-templates.md) before changing them.
