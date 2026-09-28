# Maxwell Format

`format = 'maxwell-json'`

CDC format for streaming changes from MySQL via Maxwell.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `maxwell-json.ignore-parse-errors` | `false` | Skip rows with parse errors |
| `maxwell-json.timestamp-format.standard` | `'SQL'` | `'SQL'` or `'ISO-8601'` |
| `maxwell-json.map-null-key.mode` | `'FAIL'` | `'FAIL'`, `'DROP'`, or `'LITERAL'` |
| `maxwell-json.map-null-key.literal` | `'null'` | String for null keys when mode is LITERAL |
| `maxwell-json.encode.decimal-as-plain-number` | `false` | Avoid scientific notation |
| `maxwell-json.encode.ignore-null-fields` | `false` | Omit null fields |

## Available Metadata

Access via `METADATA FROM 'value.<key>'`:

| Key | Data Type | Description |
|-----|-----------|-------------|
| `database` | `STRING NULL` | Source database name |
| `table` | `STRING NULL` | Source table name |
| `primary-key-columns` | `ARRAY<STRING> NULL` | Primary key column names |
| `ingestion-timestamp` | `TIMESTAMP_LTZ(3) NULL` | Connector processing time (`ts` field) |

## Key Notes

- Maxwell can deliver **at-least-once** on failover, causing duplicate events
- To handle duplicates: set `table.exec.source.cdc-events-duplicate = true` and define PRIMARY KEY
- Flink encodes UPDATE as separate DELETE + INSERT messages
