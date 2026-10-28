# Kafka Safe Connectors

Extends the [kafka](kafka.md) and [upsert-kafka](upsert-kafka.md) connectors (add `-safe` to the respective connector name) with Dead-letter-queue support for messages that do not deserialize (i.e. poison pill messages).

Supports all configuration options of the base kafka connector and these additional ones:

| Options                         | Default | Type    | Description                                                                                                                                         |
|---------------------------------|---------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| scan.deser-failure.handler      | none    | String  | Use `log` to output failed messages to the logger, `kafka` to output failed messages to a kafka topic, or `none` to fail the job.                   |
| scan.deser-failure.topic        | -       | String  | The topic for the dead-letter-queue that failed messages are written to. Required when the handler is configured to `kafka`.                        |
| scan.key-fields.read-from-value | true    | Boolean | `kafka-safe` only. Read the key fields from the value when it contains every key field; `false` decodes the key and drops records without a key. |

## Key Notes

The dead-letter-queue producer will use the same Kafka configuration that is provided for the Flink SQL table that reads the data.

With `key.fields`, the plain `kafka` connector silently drops records that have no key when the key format, such as `json`, produces no key row for them. When the value contains every key field (`'value.fields-include' = 'ALL'`), `kafka-safe` reads the key fields from the value instead and keeps such records, unless `scan.key-fields.read-from-value` is `false`.
