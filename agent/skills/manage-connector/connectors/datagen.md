# DataGen Connector

`'connector' = 'datagen'`

Generates synthetic data for testing.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `rows-per-second` | `10000` | Emit rate |
| `number-of-rows` | unbounded | Total rows to emit (makes table bounded) |
| `scan.parallelism` | global default | Source parallelism |

### Per-Field Options

Use `fields.<field_name>.<option>`:

| Option | Default | Description |
|--------|---------|-------------|
| `fields.#.kind` | `random` | Generator type: `random` or `sequence` |
| `fields.#.min` | type min | Minimum value (numeric types) |
| `fields.#.max` | type max | Maximum value (numeric types) |
| `fields.#.start` | - | Sequence start value |
| `fields.#.end` | - | Sequence end value (table becomes bounded) |
| `fields.#.length` | 100 (string), 3 (collections) | Length for varchar/string/bytes/array/map |
| `fields.#.var-len` | `false` | Generate variable-length strings |
| `fields.#.max-past` | `0` | Max past duration for timestamps |
| `fields.#.null-rate` | `0` | Proportion of null values (0.0-1.0) |

## Key Notes

- **Unbounded by default**: Set `number-of-rows` or use sequence generator to bound
- **Sequence generator**: When any column uses sequence, table ends when first sequence completes
- **Time types**: DATE/TIME always use current system time; TIMESTAMP/TIMESTAMP_LTZ use current time minus random `max-past`
- **Length constraints**: varchar/varbinary limited by schema definition; string/bytes default 100 (max 2^31)

## Example

```sql
CREATE TABLE MockOrders WITH (
    'connector' = 'datagen',
    'number-of-rows' = '1000',
    'fields.order_id.kind' = 'sequence',
    'fields.order_id.start' = '1',
    'fields.order_id.end' = '1000',
    'fields.amount.min' = '10',
    'fields.amount.max' = '1000'
) LIKE Orders (EXCLUDING ALL);
```
