# Kafka Engine Configuration

Apache Kafka is a streaming data platform that serves as the log engine in DataSQRL pipelines for handling data streams and event logs.

## Configuration Options

The default configuration only declares the engine; topic definitions are injected at **plan** time. Additional keys (e.g. `bootstrap.servers`) may be added under `config`.

## Example Configuration

```json
{
  "engines": {
    "kafka": {
      "config": {
        "bootstrap.servers": "localhost:9092"
      }
    }
  }
}
```

## Usage Notes

- Kafka topics are automatically created based on your SQRL table definitions
- Topic configurations are generated during the compilation process
- For custom Kafka settings, add them under the `config` section
- Kafka serves as the messaging backbone between different engines in the pipeline