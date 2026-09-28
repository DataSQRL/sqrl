# Parquet Format

`format = 'parquet'`

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `parquet.utc-timezone` | `false` | Use UTC for timestamp conversion (Hive 3.x uses UTC, Hive 0.x/1.x/2.x use local) |
| `write.int64.timestamp` | `false` | Write timestamps as int64 (Spark compatible) instead of int96 (Hive compatible) |
| `timestamp.time.unit` | `micros` | Time unit for int64 timestamps: `nanos`, `micros`, `millis` |
| `parquet.compression` | `(none)` | Compression codec: `GZIP`, `SNAPPY`, `LZO`, `BROTLI`, `LZ4`, `ZSTD` |

## Key Notes

- Default timestamp format (int96) is Hive compatible but NOT Spark compatible
- For Spark compatibility, set `write.int64.timestamp = true`
- MAP keys cannot be nullable
