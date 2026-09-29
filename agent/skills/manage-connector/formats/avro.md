# Avro Format

`format = 'avro'`

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `avro.encoding` | `binary` | `binary` or `json` encoding |
| `avro.codec` | `(none)` | Compression: `null`, `deflate`, `snappy`, `bzip2`, `xz` (filesystem only) |
| `avro.timestamp_mapping.legacy` | `true` | Set `false` for correct TIMESTAMP/TIMESTAMP_LTZ mapping |

## Key Notes

- MAP keys must be string/char/varchar type
- Nullable types mapped to Avro `union(something, null)`
- Legacy timestamp mapping (pre-1.19) incorrectly mapped both TIMESTAMP types to Avro TIMESTAMP; set `avro.timestamp_mapping.legacy = false` for correct behavior
