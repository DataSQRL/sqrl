# Debezium Format

`format = 'debezium-json'` or `format = 'debezium-avro-confluent'`

CDC format for streaming changes from MySQL, PostgreSQL, Oracle, SQL Server, MongoDB via Debezium.

## Configuration Options (JSON)

| Option | Default | Description |
|--------|---------|-------------|
| `debezium-json.schema-include` | `false` | Set `true` if Kafka Connect has `value.converter.schemas.enable` on |
| `debezium-json.ignore-parse-errors` | `false` | Skip rows with parse errors |
| `debezium-json.timestamp-format.standard` | `'SQL'` | `'SQL'` or `'ISO-8601'` |
| `debezium-json.map-null-key.mode` | `'FAIL'` | `'FAIL'`, `'DROP'`, or `'LITERAL'` |
| `debezium-json.encode.decimal-as-plain-number` | `false` | Avoid scientific notation |
| `debezium-json.encode.ignore-null-fields` | `false` | Omit null fields |

## Configuration Options (Avro)

| Option | Required | Description |
|--------|----------|-------------|
| `debezium-avro-confluent.url` | **yes** | Schema Registry URL |
| `debezium-avro-confluent.subject` | no | Subject name (default: `<topic>-value`) |
| `debezium-avro-confluent.basic-auth.user-info` | no | Basic auth credentials |
| `debezium-avro-confluent.bearer-auth.token` | no | Bearer auth token |
| `debezium-avro-confluent.ssl.*` | no | SSL keystore/truststore settings |

## Available Metadata

Access via `METADATA FROM 'value.<key>'`:

| Key | Data Type | Description |
|-----|-----------|-------------|
| `schema` | `STRING NULL` | JSON schema (if included) |
| `ingestion-timestamp` | `TIMESTAMP_LTZ(3) NULL` | Connector processing time (`ts_ms`) |
| `source.timestamp` | `TIMESTAMP_LTZ(3) NULL` | Source system event time (`source.ts_ms`) |
| `source.database` | `STRING NULL` | Source database (`source.db`) |
| `source.schema` | `STRING NULL` | Source schema (`source.schema`) |
| `source.table` | `STRING NULL` | Source table (`source.table` or `source.collection`) |
| `source.properties` | `MAP<STRING, STRING> NULL` | All source properties |

## Key Notes

- Debezium delivers **at-least-once** on failover; set `table.exec.source.cdc-events-duplicate = true` with PRIMARY KEY
- **PostgreSQL**: Table must have `REPLICA IDENTITY FULL` for UPDATE/DELETE to include all columns:
  ```sql
  ALTER TABLE <table> REPLICA IDENTITY FULL;
  ```
- Flink encodes UPDATE as separate DELETE + INSERT messages
