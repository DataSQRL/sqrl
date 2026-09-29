# Kafka Safe Connectors

Extends the [kafka](kafka.md) and [upsert-kafka](upsert-kafka.md) connectors (add `-safe` to the respective connector name) with Dead-letter-queue support for messages that do not deserialize (i.e. poison pill messages).

Supports all configuration options of the base kafka connector and these additional ones:

| Options                    | Default | Type   | Description                                                                                                                       |
|----------------------------|---------|--------|-----------------------------------------------------------------------------------------------------------------------------------|
| scan.deser-failure.handler | none    | String | Use `log` to output failed messages to the logger, `kafka` to output failed messages to a kafka topic, or `none` to fail the job. |
| scan.deser-failure.topic   | -       | String | The topic for the dead-letter-queue that failed messages are written to. Required when the handler is configured to `kafka`.      |

## Key Notes

The dead-letter-queue producer will use the same Kafka configuration that is provided for the Flink SQL table that reads the data.
