# Connecting External Data Sources and Sinks

ALWAYS USE EVENT TIME

Use `CREATE TABLE` statements to connect external data sources and sinks with your SQRL script using the `WITH` clause to provide connector configuration.

DataSQRL uses Apache Flink connectors and formats. To find a connector for your data system, use:

* **[The Official Apache Flink connectors](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/overview/)** for Kafka, Filesystem, Kinesis, and many more.
* **DataSQRL provided connectors**
  * **[Safe Kafka Source Connectors](https://github.com/DataSQRL/flink-sql-runner?tab=readme-ov-file#dead-letter-queue-support-for-kafka-sources)** which support dead-letter queues for faulty messages.
* **[Apache Flink CDC connectors](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/flink-sources/overview)** for Postgres, MySQL, Oracle, SqlServer, and other databases.

## Connector Management

The best practice for managing connectors in your DataSQRL project is to create a folder for each system that you are
connecting to and place all source or sink `CREATE TABLE` statements in separate files ending in `.table.sql` in that folder.
You can then import from and export to those sources and sinks in the SQRL script.

For example, to ingest data from the `User` and `Transaction` topics of a Kafka cluster, you would:
1. Create a sub-directory `kafka-sources` in your project directory that contains your SQRL script
2. Create two files `user.table.sql` and `transaction.table.sql`.
3. Each file contains a `CREATE TABLE` statement that defines columns for each field in the message and a `WITH` clause
   that contains the connector configuration. They will look like this:
    ```sql
    CREATE TABLE User (
      user_id BIGINT,
      user_name STRING,
      last_updated TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
      WATERMARK FOR last_updated AS last_updated - INTERVAL '1' SECOND
      WATERMARK 
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'user',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'user-consumer-group',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'avro',
    );
    ```
4. Import those sources into your SQRL script with `IMPORT kafak-sources.User;`
5. Keep sources and sinks in separate folders (e.g. `kafka-sink`)

By following this structure, you modularize your sources and sinks from your processing logic
which makes it easier to read and maintain.

## External Schemas

When ingesting data from external systems, the schema is often defined in or by those systems.
For example, Avro is a popular schema language for encoding messages in Kafka topics.
It can be very cumbersome to convert that schema to SQL and maintain that translation.

With DataSQRL, you can easily create a table that fetches the schema from a given avro schema file.
Following the example above and assuming that the schema for the `User` topic is `user.avsc`,
and that file is placed next to the `user.table.sql` file, then a `LIKE <schema-file-path>` statement can be added
to the `CREATE TABLE` statement, so in the `user.table.sql` we only need to define the metadata, the watermark,
and the connector options:
```sql
CREATE TABLE User (
  last_updated TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  WATERMARK FOR last_updated AS last_updated - INTERVAL '1' SECOND
  WATERMARK 
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
