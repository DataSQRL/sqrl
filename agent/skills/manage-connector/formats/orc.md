# ORC Format

`format = 'orc'`

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `orc.compress` | `(none)` | Compression: `SNAPPY`, `ZLIB`, `LZO`, `LZ4`, `ZSTD` |

Additional ORC table properties can be set directly (e.g., `orc.compress=SNAPPY`).

## Key Notes

- Hive compatible type mapping
- Minimal configuration needed
