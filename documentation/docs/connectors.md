# Connecting External Data Sources and Sinks

Use `CREATE TABLE` statements to connect external data sources and sinks with your SQRL script using the `WITH` clause to provide connector configuration.

DataSQRL uses Apache Flink connectors and formats. To find a connector for your data system, use:

* **[The Official Apache Flink connectors](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/overview/)** for Kafka, Filesystem, Kinesis, and many more.
* **DataSQRL provided connectors**
  * **[Safe Kafka Source Connectors](https://github.com/DataSQRL/flink-sql-runner?tab=readme-ov-file#dead-letter-queue-support-for-kafka-sources)** which support dead-letter queues for faulty messages.
* **[Apache Flink CDC connectors](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/flink-sources/overview)** for Postgres, MySQL, Oracle, SqlServer, and other databases.

## Connector Management

The best practice for managing connectors in your DataSQRL project is to place all `CREATE TABLE` statements for one data source in a single `.sqrl` file inside the `connectors` folder. This provides a modular structure for sources and sinks that supports reusability and replacing connectors for testing or different environments. [Import](sqrl-language#import-statement) those connector files into your main script.

We **strongly encourage** the use of event-time processing and ensuring that all sources are configured with a proper watermark. While processing-time is supported, only event-time ensures consistent data processing and sane, reproducible results.

When ingesting data from external data sources it is important to note the [type of table](sqrl-language#type-system) you are creating: an append-only `STREAM` (e.g. with the `filesystem` connector), `VERSIONED_STATE` retraction stream (e.g. with the `upsert-kafka` connector), or a `LOOKUP` table (e.g. with the `jdbc` connector). The table type determines what operations a table supports and how it should be processed.

Specifically, entity data is often ingested as a stream of updates. To re-create the underlying entity `VERSIONED_STATE` table, use the `DISTINCT` statement with the entity's primary key.

## External Schemas

When ingesting data from external systems, the schema is often defined in or by those systems.
For example, Avro is a popular schema language for encoding messages in Kafka topics.
It can be very cumbersome to convert that schema to SQL and maintain that translation.

With DataSQRL, you can easily create a table that fetches the schema from a given avro schema file.
Assuming that the schema for the `User` topic is `user.avsc`,
and that file is placed next to the `sources.sqrl` file in the `connectors` folder, add `LIKE <schema-file-path>` to the `CREATE TABLE` statement which populates the table schema from the avro file:
```sql
CREATE TABLE User (
  last_updated TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  WATERMARK FOR last_updated AS last_updated - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'user',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'user-consumer-group',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro',
) LIKE `user.avsc`;
```

:::info
We can even include files from other folder via relative path, but in most cases it makes sense to put the schema file next to the table sql.
:::