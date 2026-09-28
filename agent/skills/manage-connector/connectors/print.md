# Print Connector

`'connector' = 'print'`

Writes rows to stdout/stderr for debugging.

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `print-identifier` | `(none)` | Prefix for output messages |
| `standard-error` | `false` | Write to stderr instead of stdout |
| `sink.parallelism` | upstream | Sink parallelism |

## Key Notes

- **Output location**: Prints to task manager logs, not driver/client output
- **Output format**: `$row_kind(f0,f1,f2...)` e.g., `+I(1,1)` for insert, `-D(1,1)` for delete
- **Parallel output**: With parallelism > 1, output prefixed with task ID: `identifier:taskId> output`
