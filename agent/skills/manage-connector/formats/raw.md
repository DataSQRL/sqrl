# Raw Format

`format = 'raw'`

Reads/writes raw bytes as a single column.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `raw.charset` | `UTF-8` | Character encoding for strings |
| `raw.endianness` | `big-endian` | Byte order for numeric types: `big-endian` or `little-endian` |

## Key Notes

- Single column only (reads entire message as one value)
- **Avoid with `upsert-kafka`**: null values become tombstones (DELETE) which may cause data loss
- Useful for raw log ingestion with UDF parsing
- Built-in, no additional dependencies required
