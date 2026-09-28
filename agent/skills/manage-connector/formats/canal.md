# Canal Format

`format = 'canal-json'`

CDC format for streaming changes from MySQL via Canal.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `canal-json.ignore-parse-errors` | `false` | Skip rows with parse errors |
| `canal-json.timestamp-format.standard` | `'SQL'` | `'SQL'` or `'ISO-8601'` |
| `canal-json.map-null-key.mode` | `'FAIL'` | `'FAIL'`, `'DROP'`, or `'LITERAL'` |
| `canal-json.map-null-key.literal` | `'null'` | String for null keys when mode is LITERAL |
| `canal-json.encode.decimal-as-plain-number` | `false` | Avoid scientific notation |
| `canal-json.database.include` | `(none)` | Regex to filter by database name |
| `canal-json.table.include` | `(none)` | Regex to filter by table name |

## Available Metadata

Access via `METADATA FROM 'value.<key>'`:

| Key | Data Type | Description |
|-----|-----------|-------------|
| `database` | `STRING NULL` | Source database name |
| `table` | `STRING NULL` | Source table name |
| `sql-type` | `MAP<STRING, INT> NULL` | SQL type mapping |
| `pk-names` | `ARRAY<STRING> NULL` | Primary key column names |
| `ingestion-timestamp` | `TIMESTAMP_LTZ(3) NULL` | Connector processing time (`ts` field) |
| `event-timestamp` | `TIMESTAMP_LTZ(3) NULL` | MySQL event time (`es` field) |

## Key Notes

- Canal delivers **at-least-once** on failover, causing duplicate events
- To handle duplicates: set `table.exec.source.cdc-events-duplicate = true` and define PRIMARY KEY
- Flink encodes UPDATE as separate DELETE + INSERT messages (cannot combine UPDATE_BEFORE/AFTER)
