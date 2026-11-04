# Kafka Engine Configuration

| Key                                | Type            | Default  | Description                                                                   |
|------------------------------------|-----------------|----------|-------------------------------------------------------------------------------|
| `retention`                        | **string/null** | `null`   | Topic retention time (e.g., `"7d"`, `"24h"`) or indefinite when `null`        |
| `watermark`                        | **string**      | `"0 ms"` | Watermark delay for non-transactional Kafka tables                            |
| `transaction-watermark`            | **string**      | `"0 ms"` | Watermark delay for transactional Kafka tables                                |
| `use-source-watermark`             | **boolean**     | `false`  | Use Flink `SOURCE_WATERMARK()` for non-transactional Kafka tables             |
| `use-transaction-source-watermark` | **boolean**     | `false`  | Use Flink `SOURCE_WATERMARK()` for transactional Kafka tables                 |
| `num-partitions`                   | **integer**     | `1`      | Partition count for generated Kafka topics                                    |
| `replication-factor`               | **integer**     | `3`      | Replication factor for generated Kafka topics; cannot exceed the broker count |

## Example Configuration

```json
{
  "engines": {
    "kafka": {
      "retention": "14d",
      "watermark": "2 sec",
      "transaction-watermark": "10 sec",
      "use-source-watermark": true,
      "use-transaction-source-watermark": true,
      "num-partitions": 4,
      "replication-factor": 3
    }
  }
}
```

Most Kafka producer and consumer settings belong in the Kafka connector templates. The package schema does **not** accept an `engines.kafka.config` object. If client settings need adjustment, use the official Kafka [consumer](https://kafka.apache.org/42/configuration/consumer-configs/) or [producer](https://kafka.apache.org/42/configuration/producer-configs/) documentation, then update the appropriate [connector template](connector-templates.md). Separate formats for the message key and value are configured there as well (see [Kafka key and value formats](connector-templates.md#kafka-key-and-value-formats)).

Source watermarks apply only to mutation tables with a `timestamp` metadata column and require the `kafka-safe` or `upsert-kafka-safe` connector. When enabled, they generate `WATERMARK FOR <timestamp-column> AS SOURCE_WATERMARK()` instead of applying the corresponding delay.

Only the mutation table writes its Kafka topic. Another writer mixes its rows into the mutation, and a writer that reads the mutation reads its own output in an endless loop. When the `kafka` [connector template](connector-templates.md) gives a Kafka table that the compiler creates for Flink the topic of a mutation table, the compiler puts `<table-name>-subscription` in place of `${sqrl:table-name}` in the template's `topic` instead, so `acme-${sqrl:table-name}` gives `acme-Events-subscription`. This happens, for example, when a `SUBSCRIBE` function reads a mutation table (a `CREATE TABLE` with the `engine(kafka)` hint) directly: with the default templates, the mutation `Events` keeps the topic `Events`, and the subscription reads the topic `Events-subscription`. If the template gives a mutation topic again, for example with a fixed `topic` without `${sqrl:table-name}`, the compile fails with an error. An `EXPORT` statement to `log.<name>` or `logger.<name>` that gets the topic of a mutation table also fails with an error, for example `EXPORT Orders TO log.Events` when `Events` is a mutation table: use a different name in the `EXPORT` statement. All other topics keep the name from the template. The compiler does not check an `EXPORT` statement that writes directly into a mutation table, such as `EXPORT _Echo TO Events`: when the exported query reads that mutation table, the export loops, so do not write one.
