# Connector Template Configuration

Connector templates defined under the `connectors` field in the `package.json` configuration determine how table data is mapped and exchanged between data systems.
The connector templates use Flink SQL connector configuration options which are mapped to the configuration for each engine.

The defaults below are the complete set DataSQRL applies. Add or overwrite **individual fields** — a template you declare is merged into the default, not substituted for it, so name only the fields you are changing.

```json
{
  "connectors": {
    "kafka-mutation": {
      "connector": "kafka-safe",
      "format": "flexible-json",
      "properties.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
      "properties.group.id": "${KAFKA_GROUP_ID}",
      "properties.auto.offset.reset": "earliest",
      "properties.compression.type": "zstd",
      "topic": "${sqrl:table-name}",
      "flexible-json.timestamp-format.standard": "ISO-8601"
    },
    "kafka": {
      "connector": "kafka-safe",
      "format": "flexible-json",
      "properties.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
      "properties.group.id": "${KAFKA_GROUP_ID}",
      "properties.compression.type": "zstd",
      "topic": "${sqrl:table-name}",
      "flexible-json.timestamp-format.standard": "ISO-8601"
    },
    "iceberg": {
      "connector": "iceberg",
      "catalog-name": "default_catalog",
      "catalog-table": "${sqrl:table-name}",
      "warehouse": "sqrl_iceberg_data",
      "format-version": 2,
      "write.distribution-mode": "hash",
      "commit.retry.num-retries": "20",
      "commit.retry.min-wait-ms": "100",
      "commit.retry.max-wait-ms": "5000"
    },
    "iceberg-maintenance": {
      "compaction.enabled": "true",
      "write.metadata.delete-after-commit.enabled": "true",
      "write.metadata.previous-versions-max": "10",
      "history.expire.max-snapshot-age-ms": "86400000"
    },
    "postgres": {
      "connector": "jdbc-sqrl",
      "username": "${POSTGRES_USERNAME}",
      "password": "${POSTGRES_PASSWORD}",
      "url": "jdbc:postgresql://${POSTGRES_AUTHORITY}",
      "driver": "org.postgresql.Driver",
      "table-name": "${sqrl:table-name}"
    },
    "print": {
      "connector": "print",
      "print-identifier": "${sqrl:table-name}"
    }
  }
}
```
