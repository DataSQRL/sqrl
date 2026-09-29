# Protobuf Format

`format = 'protobuf'`

Requires pre-compiled protobuf Java classes in classpath.

## Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `protobuf.message-class-name` | **yes** | - | Full class name (e.g., `com.example.MyMessage` or `OuterClass$InnerMessage`) |
| `protobuf.ignore-parse-errors` | no | `false` | Skip rows with parse errors |
| `protobuf.read-default-values` | no | `false` | Return proto defaults instead of null for missing fields (slower) |
| `protobuf.write-null-string-literal` | no | `""` | String to use for null in arrays/maps |

## Key Notes

- Arrays/maps cannot contain null values (auto-converted to defaults: `0` for numbers, `""` for strings, `false` for bool)
- Supports proto2, proto3, and Editions (2023, 2024)
- OneOf fields: later fields override earlier ones when serializing
- `google.protobuf.timestamp` maps to `ROW<seconds BIGINT, nanos INT>`
- Enum values map to STRING or INT depending on Flink column type
