# OGG Format

`format = 'ogg-json'`

CDC format for streaming changes from Oracle via Oracle GoldenGate.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `ogg-json.ignore-parse-errors` | `false` | Skip rows with parse errors |
| `ogg-json.timestamp-format.standard` | `'SQL'` | `'SQL'` or `'ISO-8601'` |
| `ogg-json.map-null-key.mode` | `'FAIL'` | `'FAIL'`, `'DROP'`, or `'LITERAL'` |
| `ogg-json.map-null-key.literal` | `'null'` | String for null keys when mode is LITERAL |
| `ogg-json.encode.ignore-null-fields` | `false` | Omit null fields |

## Available Metadata

Access via `METADATA FROM 'value.<key>'`:

| Key | Data Type | Description |
|-----|-----------|-------------|
| `table` | `STRING NULL` | Fully qualified table name (CATALOG.SCHEMA.TABLE) |
| `primary-keys` | `ARRAY<STRING> NULL` | Primary key columns (requires `includePrimaryKeys = true` in OGG config) |
| `ingestion-timestamp` | `TIMESTAMP_LTZ(6) NULL` | Connector processing time (`current_ts`) |
| `event-timestamp` | `TIMESTAMP_LTZ(6) NULL` | Oracle event time (`op_ts`) |

## Key Notes

- Set `includePrimaryKeys = true` in OGG Kafka Handler config to get primary key metadata
- Flink encodes UPDATE as separate DELETE + INSERT messages
