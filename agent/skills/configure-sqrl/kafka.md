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

Most Kafka producer and consumer settings belong in the Kafka connector templates. The package schema does **not** accept an `engines.kafka.config` object. If client settings need adjustment, use the official Kafka [consumer](https://kafka.apache.org/42/configuration/consumer-configs/) or [producer](https://kafka.apache.org/42/configuration/producer-configs/) documentation, then update the appropriate [connector template](connector-templates.md).

Source watermarks apply only to mutation tables with a `timestamp` metadata column and require the `kafka-safe` or `upsert-kafka-safe` connector. When enabled, they generate `WATERMARK FOR <timestamp-column> AS SOURCE_WATERMARK()` instead of applying the corresponding delay.
