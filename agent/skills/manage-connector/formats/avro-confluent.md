# Avro Confluent Format

`format = 'avro-confluent'`

For use with Confluent Schema Registry. Only works with `kafka` or `upsert-kafka` connectors.

## Configuration Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `avro-confluent.url` | **yes** | - | Schema Registry URL |
| `avro-confluent.subject` | no | `<topic>-value` or `<topic>-key` | Subject name for schema registration |
| `avro-confluent.basic-auth.credentials-source` | no | - | Basic auth credentials source |
| `avro-confluent.basic-auth.user-info` | no | - | Basic auth user:password |
| `avro-confluent.bearer-auth.credentials-source` | no | - | Bearer auth credentials source |
| `avro-confluent.bearer-auth.token` | no | - | Bearer auth token |
| `avro-confluent.ssl.keystore.location` | no | - | SSL keystore path |
| `avro-confluent.ssl.keystore.password` | no | - | SSL keystore password |
| `avro-confluent.ssl.truststore.location` | no | - | SSL truststore path |
| `avro-confluent.ssl.truststore.password` | no | - | SSL truststore password |
| `avro-confluent.schema` | no | - | Explicit Avro schema (otherwise derived from table) |
| `avro-confluent.properties` | no | - | Additional Schema Registry properties |

## Key Notes

- Schema evolution in Kafka keys is rarely backward/forward compatible due to hash partitioning
- Use `key.fields-prefix` to avoid column name clashes when both key and value have same field names
- For non-Kafka connectors (e.g., filesystem), `avro-confluent.subject` is required when used as sink
