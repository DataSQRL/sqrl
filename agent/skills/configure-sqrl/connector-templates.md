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

## Kafka key and value formats

The `kafka` and `kafka-mutation` templates set one `format` for the message key and value. For separate formats, add `key.format` and/or `value.format` to the template, or to the `WITH` options of an internal table:

- They take precedence over `format`. The default `format` stays in the merged template, so do not try to remove it.
- A table with a message key (`PARTITIONED BY` or a `PRIMARY KEY`) uses `key.format` for the key, otherwise `format`, otherwise `value.format`. A table without a message key drops `key.format`.
- Options of `format` (e.g. `flexible-json.timestamp-format.standard`) only reach the side that uses that format. Prefix an option with `key.` or `value.` to set it for one side.
- Use `format` when both sides share a format; the compiler notes when `key.format` and `value.format` are equal.
- Keep mutation tables on `json` or `flexible-json`: the server writes mutations as JSON, and the compiler warns for other formats.
