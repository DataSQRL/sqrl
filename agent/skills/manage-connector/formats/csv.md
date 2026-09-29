# CSV Format

`format = 'csv'`

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `csv.field-delimiter` | `,` | Field delimiter (single char, supports `\t`, unicode like `U&'\0001'`) |
| `csv.quote-character` | `"` | Quote character for enclosing fields |
| `csv.disable-quote-character` | `false` | Disable quoting entirely |
| `csv.allow-comments` | `false` | Ignore lines starting with `#` |
| `csv.ignore-parse-errors` | `false` | Skip rows with parse errors |
| `csv.array-element-delimiter` | `;` | Delimiter for array/row elements |
| `csv.escape-character` | `(none)` | Escape character |
| `csv.null-literal` | `(none)` | String to interpret as null |
| `csv.write-bigdecimal-in-scientific-notation` | `true` | Write decimals in scientific notation |

## Prefer richer formats for production

Flink's CSV support is limited and lightly maintained, so CSV is suited to **test data**. Prefer `parquet`, `avro`, or `flexible-json` for production sources where practical. CSV in production is fine when a customer requires it — just be
extra defensive about parsing (below), since ragged real-world data bites hardest
there.

## Ragged rows: set `csv.ignore-parse-errors` on sources

Provided CSV test data is often ragged — trailing delimiters, extra empty
columns, the odd malformed row. Flink's CSV reader is **strict by default**, so a
single row with more fields than the schema fails the **whole** job at runtime
(it still compiles cleanly):
```
Too many entries: expected at most 9 (value #9 (0 chars) "")
```

The empty extra value (`(0 chars) ""`) is the tell — a row carried one more field
than the 9-column schema, almost always a trailing delimiter.

Set `'csv.ignore-parse-errors' = 'true'` on CSV **source** connectors so bad rows
are skipped instead of crashing the run. Apply it to CSV **test** sources by
default, and to any CSV **production** source too — that's where messy real-world
data bites hardest.

Trade-off: skipped rows drop **silently** — this is a runtime safety net, not a
substitute for a correct schema. A wrong column count can hide as "fewer rows"
instead of a loud error. So invoke the `/data-observation` skill and inspect the real data first to
get the column count, delimiter, and quoting right; use `ignore-parse-errors` only
for the ragged tail. If *every* row has a trailing delimiter, add the missing
nullable trailing column to the schema instead.

## Key Notes

- `BINARY/VARBINARY` encoded as base64
- When using `csv.allow-comments`, also enable `csv.ignore-parse-errors` for empty rows
