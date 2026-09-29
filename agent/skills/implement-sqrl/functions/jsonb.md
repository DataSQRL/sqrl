# Jsonb Functions

| Function | Description | Example |
|----------|-------------|---------|
| `to_jsonb` | Parses a JSON string or Flink object (e.g., `Row`, `Row[]`) into a JSON object. | `to_jsonb('{"name":"Alice"}') → JSON object` |
| `jsonb_to_string` | Serializes a JSON object into a JSON string. | `jsonb_to_string(to_jsonb('{"a":1}')) → '{"a":1}'` |
| `jsonb_object` | Constructs a JSON object from key-value pairs. Keys must be strings. | `jsonb_object('a', 1, 'b', 2) → {"a":1,"b":2}` |
| `jsonb_array` | Constructs a JSON array from multiple values or JSON objects. | `jsonb_array(1, 'a', to_jsonb('{"b":2}')) → [1,"a",{"b":2}]` |
| `jsonb_extract` | Extracts a value from a JSON object using a JSONPath expression. Optionally specify default value. | `jsonb_extract(to_jsonb('{"a":1}'), '$.a') → 1` |
| `jsonb_query` | Executes a JSONPath query on a JSON object and returns the result as a JSON string. | `jsonb_query(to_jsonb('{"a":[1,2]}'), '$.a') → '[1,2]'` |
| `jsonb_exists` | Returns `TRUE` if a JSONPath exists within a JSON object. | `jsonb_exists(to_jsonb('{"a":1}'), '$.a') → TRUE` |
| `jsonb_concat` | Merges two JSON objects. If keys overlap, the second object's values are used. | `jsonb_concat(to_jsonb('{"a":1}'), to_jsonb('{"b":2}')) → {"a":1,"b":2}` |
| `jsonb_array_agg` | Aggregate function that accumulates values into a JSON array. | `SELECT jsonb_array_agg(col) FROM tbl` |
| `jsonb_object_agg` | Aggregate function that accumulates key-value pairs into a JSON object. | `SELECT jsonb_object_agg(key_col, val_col) FROM tbl` |
