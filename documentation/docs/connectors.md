# Connecting External Data Sources and Sinks

Use `CREATE TABLE` statements to connect external data sources and sinks with your SQRL script using the `WITH` clause to provide connector configuration.

DataSQRL uses Apache Flink connectors and formats. To find a connector for your data system, use:

* **[The Official Apache Flink connectors](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/connectors/table/overview/)** for Kafka, Filesystem, Kinesis, and many more.
* **DataSQRL provided connectors**
  * **[Safe Kafka Source Connectors](https://github.com/DataSQRL/flink-sql-runner?tab=readme-ov-file#dead-letter-queue-support-for-kafka-sources)** which support dead-letter queues for faulty messages.
* **[Apache Flink CDC connectors](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/flink-sources/overview/)** for Postgres, MySQL, Oracle, SqlServer, and other databases.

## Connector Management

The best practice for managing connectors in your DataSQRL project is to place all `CREATE TABLE` statements for one data source in a single `.sqrl` file inside the `connectors` folder. This provides a modular structure for sources and sinks that supports reusability and replacing connectors for testing or different environments. [Import](sqrl-language#import-statement) those connector files into your main script.

We **strongly encourage** the use of event-time processing and ensuring that all sources are configured with a proper watermark. While processing-time is supported, only event-time ensures consistent data processing and sane, reproducible results.

When ingesting data from external data sources it is important to note the [type of table](sqrl-language#type-system) you are creating: an append-only `STREAM` (e.g. with the `filesystem` connector), `VERSIONED_STATE` retraction stream (e.g. with the `upsert-kafka` connector), or a `LOOKUP` table (e.g. with the `jdbc` connector). The table type determines what operations a table supports and how it should be processed.

Specifically, entity data is often ingested as a stream of updates. To re-create the underlying entity `VERSIONED_STATE` table, use the `DISTINCT` statement with the entity's primary key.

## LIKE Clause for Schema Loading

DataSQRL supports automatic schema loading from external schema files using the `LIKE` clause. This feature eliminates the need to manually define column definitions when the schema already exists in a supported format.

```sql
CREATE TABLE MyTable (
  ...
) WITH (
  ...
) LIKE 'mytable.avsc';
```

The `LIKE` clause:
- **Automatically populates** column definitions from the referenced schema file
- **Maintains schema consistency** between your data pipeline and external systems
- **Reduces maintenance overhead** by eliminating manual schema translations
- **Supports relative paths** to schema files in your project structure

DataSQRL currently supports loading schemas from:
- **Avro schema files** (`.avsc`)

Assuming you have an Avro schema file `user.avsc` for a Kafka topic:

```sql
CREATE TABLE User (
  last_updated TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  WATERMARK FOR last_updated AS last_updated - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  ...
) LIKE 'user.avsc';
```

In this example:
- The `LIKE 'user.avsc'` clause loads all column definitions from the Avro schema
- You add **metadata columns** (like `last_updated`) and **watermark specifications**
- The **connector configuration** remains in the `WITH` clause as usual

### Inferring Schema from Data Files

To automatically discover the schema of a JSONL or CSV file, add the filename in the `LIKE` clause.
In addition to generating the table columns based on the inferred schema of the data, this also configures the `filesystem` connector to access the data.

For example, suppose you have a `users.jsonl` file in the `connectors` directory, you can define the `User` table simply as:

```sql
CREATE TABLE User (
  WATERMARK FOR last_updated AS last_updated - INTERVAL '1' SECOND
) WITH (
  'source.monitor-interval' = '10 sec', -- remove for batch processing
) LIKE 'users.jsonl';
```

This syntax is useful when building DataSQRL projects from data files since it eliminates the manual schema creation.
