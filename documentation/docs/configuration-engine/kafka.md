# Kafka Engine Configuration

Apache Kafka is a streaming data platform that serves as the log engine in DataSQRL pipelines for handling data streams and event logs.

## Configuration Options

| Key                    | Type        | Default   | Description                                                             |
|------------------------|-------------|-----------|-------------------------------------------------------------------------|
| `retention`            | **string**  | `null`    | Topic retention time (e.g., "7d", "24h") or indefinite when `null`      |
| `watermark`            | **string**  | `"0 ms"`  | Watermark delay for event time processing                               |
| `transaction-watermark`| **string**  | `"0 ms"`  | Watermark delay for event time processing when transactions are enabled |

Additional custom Kafka settings can be added under the `config` section.

## Example Configuration

```json
{
  "engines": {
    "kafka": {
      "retention": "14d",
      "watermark": "2 sec",
      "transaction-watermark": "10 sec",
      "config": {
        "auto.offset.reset": "earliest"
      }
    }
  }
}
```

## Usage Notes

- Kafka topics are automatically created based on your SQRL table definitions
- Topic configurations are generated during the compilation process
- Retention settings control how long data is stored in Kafka topics
- Watermarks are used for handling late-arriving events in stream processing
- Kafka serves as the messaging backbone between different engines in the pipeline

<!--EXTENDED-->

### Internal Environment Variables

When running pipelines with the DataSQRL `run` command, the following environment variables are used
in the configuration:

* `KAFKA_BOOTSTRAP_SERVERS`
* `KAFKA_GROUP_ID`