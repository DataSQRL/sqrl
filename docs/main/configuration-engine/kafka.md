# Kafka Engine Configuration

Apache Kafka is a streaming data platform that serves as the log engine in DataSQRL pipelines for handling data streams and event logs.

## Configuration Options

| Key                                | Type        | Default  | Description                                                             |
|------------------------------------|-------------|----------|-------------------------------------------------------------------------|
| `retention`                        | **string** or **null** | `null`   | Topic retention time (e.g., "7d", "24h"); `null` or `"-1"` retains data indefinitely |
| `watermark`                        | **string**  | `"0 ms"` | Watermark delay for event time processing                               |
| `transaction-watermark`            | **string**  | `"0 ms"` | Watermark delay for event time processing when transactions are enabled |
| `use-source-watermark`             | **boolean** | `false`  | Use Flink's `SOURCE_WATERMARK()` for non-transactional Kafka tables     |
| `use-transaction-source-watermark` | **boolean** | `false`  | Use Flink's `SOURCE_WATERMARK()` for transactional Kafka tables         |
| `num-partitions`                   | **integer** | `1`      | Number of partitions for generated Kafka topics                         |
| `replication-factor`               | **integer** | `3`      | Replication factor for generated Kafka topics                           |

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

## Usage Notes

- Kafka topics are automatically created based on your SQRL table definitions
- Topic configurations are generated during the compilation process
- `kafka.json` lists the topics of internal Kafka tables that a [standalone included project](https://docs.datasqrl.com/docs/main/configuration.md#include-other-sources) defines under `includedTopics`, with only their names, because that project owns them and decides their settings
- Retention settings control how long data is stored in Kafka topics
- Ensure `replication-factor` does not exceed the number of brokers in the target Kafka cluster
- Watermarks are used for handling late-arriving events in stream processing
- Source watermarks apply only to mutation tables with a `timestamp` metadata column. When enabled,
  they generate `WATERMARK FOR <timestamp-column> AS SOURCE_WATERMARK()` instead of using the
  corresponding watermark delay. It is only supported by our `kafka-safe` and `upsert-kafka-safe` connectors.
- Kafka serves as the messaging backbone between different engines in the pipeline

## Key and Value Formats

The `kafka` and `kafka-mutation` [connector templates](https://docs.datasqrl.com/docs/main/configuration.md#connector-templates-connectors) set one `format` (`flexible-json` by default) for the message key and value. To use separate formats, set `key.format` and `value.format` in the template, or in the `WITH` options of an internal table:

```json
{
  "connectors": {
    "kafka-mutation": {
      "key.format": "json",
      "value.format": "flexible-json"
    }
  }
}
```

- `key.format` and `value.format` take precedence over `format`, which the default template always contains.
- Tables with a message key (`PARTITIONED BY` or a primary key) use `key.format` for the key, otherwise `format`, otherwise `value.format`. Tables without a message key drop `key.format`.
- Options of `format`, such as `flexible-json.timestamp-format.standard`, apply only to the key or value that uses that format. Set an option for one side with a `key.` or `value.` prefix, for example `value.flexible-json.timestamp-format.standard`.
- When `key.format` and `value.format` are the same, the compiler notes that `format` is enough.
- The server writes mutations as JSON, so the compiler warns when a mutation table uses a format other than `json` or `flexible-json`.

### Internal Environment Variables

When running pipelines with the DataSQRL `run` command, the following environment variables are used
in the configuration:

* `KAFKA_BOOTSTRAP_SERVERS`
* `KAFKA_GROUP_ID`
