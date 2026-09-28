# JDBC Connector

`'connector' = 'jdbc'`

Reads/writes data via JDBC. Commonly used for lookup joins or writing results to relational databases.

## Configuration Options

### Required Options

| Option | Description |
|--------|-------------|
| `url` | JDBC connection URL |
| `table-name` | Target table name in the database |

### Connection Options

| Option | Default | Description |
|--------|---------|-------------|
| `driver` | auto-derived | JDBC driver class (auto-detected from URL) |
| `username` | `(none)` | Database username |
| `password` | `(none)` | Database password |
| `connection.max-retry-timeout` | `60s` | Max timeout between connection retries |

### Source/Scan Options

| Option | Default | Description |
|--------|---------|-------------|
| `scan.partition.column` | `(none)` | Column for partitioning parallel reads |
| `scan.partition.num` | `(none)` | Number of partitions |
| `scan.partition.lower-bound` | `(none)` | Smallest value of first partition |
| `scan.partition.upper-bound` | `(none)` | Largest value of last partition |
| `scan.fetch-size` | `0` | Rows fetched per round trip (0 = driver default) |
| `scan.auto-commit` | `true` | Enable auto-commit on JDBC driver |

### Sink Options

| Option | Default | Description |
|--------|---------|-------------|
| `sink.buffer-flush.max-rows` | `100` | Max buffered records before flush |
| `sink.buffer-flush.interval` | `1s` | Flush interval |
| `sink.max-retries` | `3` | Max retry attempts for failed writes |
| `sink.parallelism` | upstream | Sink parallelism |

### Lookup Join Options

| Option | Default | Description |
|--------|---------|-------------|
| `lookup.cache` | `NONE` | `NONE` or `PARTIAL` |
| `lookup.partial-cache.max-rows` | `(none)` | Max cached rows |
| `lookup.partial-cache.expire-after-write` | `(none)` | TTL after cache insertion |
| `lookup.partial-cache.expire-after-access` | `(none)` | TTL after cache access |
| `lookup.partial-cache.cache-missing-key` | `true` | Cache empty results for missing keys |
| `lookup.max-retries` | `3` | Max retry attempts for lookup failures |

## Key Notes

- **Upsert mode**: With PRIMARY KEY defined, uses database-specific upsert syntax (MySQL: `INSERT ON DUPLICATE KEY`, PostgreSQL: `INSERT ON CONFLICT`, Oracle/SQL Server: `MERGE`)
- **Append mode**: Without PRIMARY KEY, all records inserted as new rows
- **Partitioned scan**: All partition options (`column`, `num`, `lower-bound`, `upper-bound`) must be specified together
- **Lookup cache**: Improves temporal join performance by caching on TaskManagers. Always enable when expecting more than 10 records/sec on average.

[Reference Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/jdbc/)
