# JSON Format

`'format' = 'json'` or `'format' = 'flexible-json'`

Prefer `flexible-json`, which supports nested JSON in export and the SQRL-specific types.

## Option prefix

Flink prefixes every format option with the format identifier, so the same option is spelled differently for the two formats:

| `'format' = 'json'` | `'format' = 'flexible-json'` |
|---|---|
| `'json.timestamp-format.standard' = 'ISO-8601'` | `'flexible-json.timestamp-format.standard' = 'ISO-8601'` |

A `json.` key on a `flexible-json` table (or the other way round) is rejected at compile time as an unsupported option. When the format is set through `'value.format'` (Kafka), the key carries that prefix too: `'value.flexible-json.timestamp-format.standard'`.

## Configuration Options

Listed with the `json.` prefix; write `flexible-json.` instead for `flexible-json`.

| Option | Default | Description |
|--------|---------|-------------|
| `json.fail-on-missing-field` | `false` | Fail if field is missing |
| `json.ignore-parse-errors` | `false` | Skip rows with parse errors, set fields to null |
| `json.timestamp-format.standard` | `'SQL'` | `'SQL'` (yyyy-MM-dd HH:mm:ss) or `'ISO-8601'` (yyyy-MM-ddTHH:mm:ss) |
| `json.map-null-key.mode` | `'FAIL'` | `'FAIL'`, `'DROP'`, or `'LITERAL'` for null map keys |
| `json.map-null-key.literal` | `'null'` | String to replace null keys when mode is LITERAL |
| `json.encode.decimal-as-plain-number` | `false` | Write `0.000000027` instead of `2.7E-8` |
| `json.encode.ignore-null-fields` | `false` | Omit null fields in output |

## Key Notes

- Supports top-level JSON arrays (automatically explodes into multiple rows)
- `BINARY/VARBINARY` encoded as base64 strings
- Append-only streams only; use CDC formats (debezium, canal) for retract/upsert streams
