# Connectors (Source & Sink)

To resolve `IMPORT` and `EXPORT` statements that ingest data from and write data to external systems, DataSQRL reads connector configuration files from a package. Packages are either local directories or downloaded from the repository, with the former taking precedence. Packages contain connector configurations.

For example, the statement `IMPORT mypackage.MyTable;` imports the table `MyTable` from the package `mypackage`. For this import to resolve, the connector configuration for `MyTable` needs to present in the package.

To connect to an external system like streaming platforms (e.g. Apache Flink), lake houses (e.g. Apache Iceberg), or databases (e.g. PostgreSQL), you create two configuration files:

1. **Table Configuration**: A configuration file that specifies how to ingest the data into a table and how to connect to the external system. This file is named `MyTable.table.json`.
2. **Schema Definition**: A schema that defines the structure of the data. DataSQRL supports multiple schema languages. Pick the schema language that the external system uses to make sure the data is aligned. The filename depends on the schema language - see below.

## Table Configuration

The table configuration is JSON file that has the name `MyTable.table.json` where `MyTable` is the name of the table that's used in the `IMPORT` statement.

The table configuration has three sections for defining the table properties, connector configuration, and - optionally - metadata columns.

An example table configuration file is shown below followed by the documentation for each section.

```json
{
  "version": 1,
  "table" : {
    "type" : "source",
    "primary-key" : ["id", "time"],
    "timestamp" : "_source_time",
    "watermark-millis" : "0"
  },
  "flink" : {
    "format" : "avro",
    "bootstrap.servers": "${BOOTSTRAP_SERVERS}",
    "group.id": "datasqrl-orders",
    "connector" : "kafka"
  },
  "metadata" : {
    "_source_time" : {
      "attribute" : "timestamp",
      "type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)"
    }
  }
}
```

### Table Properties

The `table` section of the table configuration specifies the table properties that are used by the DataSQRL planner to validate SQRL scripts and generate efficient data pipelines.

The `type` of a table can be either `source`, `sink`, or `source_and_sink` if it can be used as both.<br />

The `primary-key` specifies the column or list of columns that uniquely identifies a single record in the table. Note, that when the table is a changelog or CDC stream for an entity table, the primary key should uniquely identify each record in the stream and not the underlying table. For example, if you consume a CDC stream for a `Customer` entity table with primary key `customerid` the primary key for the resulting CDC stream should include the timestamp of the change, e.g. `[customerid, lastUpdated]`.

The `timestamp` field specifies the (single) timestamp column for a source stream which has the event time of a single stream record. `watermark-millis` defines the number of milliseconds that events/records can arrive late for consistent processing. Set this to `1` if events are perfectly ordered in time and to `0` if the timestamp is monotonically increasing (i.e. it's perfectly ordered and no two events have the same timestamp). <br />
Alternatively, you can also use processing time for event processing by removing the `watermark-millis` field and adding the processing time as metadata (see below), which means using the system clock of the machine processing the data and not the timestamp of the record. We highly recommend you use event time and not processing time for consistent, reproducible results. <br />
Timestamp and watermark are only used for sources.

### Connector Configuration

The connector configuration specifies how the stream engine connects to the source or sink and how it reads or writes the data. The connector configuration is specific to the configured stream processing engine that DataSQRL compiles to, and the section of the configuration is named after the engine. In the example above, the connector configuration is for the `flink` engine.

The connector configuration is passed through to the stream engine. Check the documentation for the stream processing engine you are using for how to configure the connector:

* [**Flink Connector Configuration**](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/): Make sure you use the connector configuration that's compatible with the version of Flink you are compiling to.

### Metadata Columns

Some connectors expose optional metadata that can included with the data as additional columns. Those are defined in the `metadata` section of the table configuration.

The fields under `metadata` are the column names that are added to the table. Those need to be unique and not clash with the columns in the data. The `attribute` defines either a) the name of an attribute that the connector exposes or b) a function call.

For a connector attribute, look up the name of the attribute in the connector configuration. Additionally, you need to specify the type of the attribute as `type`. <br />
In the example above, we add the Kafka timestamp as the column `_source_time` to the table with a timestamp `type`. The Kafka timestamp is exposed by the Flink Kafka connector under the `timestamp` attribute.

For a function call, you specify the full function invocation as the attribute. You don't need to specify a type. <br />
For example, to use processing time you need to add the processing time as a column with the following metadata definition:

```json
  "metadata" : {
    "_source_time" : {
      "attribute" : "proctime()"
    }
  }
```

## Schema Definition

The schema defines the structure of the data and is used to derive the row type of the imported or exported table.

If the source system has a schema, it is best to use the source schema directly to avoid mismappings. For example, if you use Avro schema registry with Kafka, download the Avro schema and place it into the package. 

### Avro

To use Avro schema, take the Avro schema file and place it into the package with the table configuration file and name it `MyTable.avsc` where `MyTable` the name of your table.

You don't need to make any modifications to your Avro schema file.

### SQL

:::note
SQL schema is currently experimental, behind a feature-flag, and does not support nested data.
:::

To use SQL schema, place the `CREATE TABLE` statement for the table data in a file named `MyTable.sql`. 

```sql title=MyTable.sql
CREATE TABLE Customer (
  id INT NOT NULL,
  name VARCHAR(100),
  birthdate DATE,
  email VARCHAR(100),
  lastUpdated TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)
);
```

Note, that the primary key must be defined in the table configuration and not in the schema SQL file. Any primary key definition in the SQL file will be ignored.

### YAML

DataSQRL supports a flexible schema format in YAML format.
DataSQRL schema is simple, accommodates semi-structured data, supports schema evolution, and provides testing capabilities. 

YAML schema files end in `.schema.yml`, e.g. `MyTable.schema.yaml`. To get flexible schema capabilities in DataSQRL for JSON source data, you also need to configure the format in the connector to `flexible-json`. The flexible json format is more robust to input data and supports JSON natively, unlike Flink's default `json` format.  

DataSQRL schema is the default schema used for schema-less sources like JSON.


#### Example DataSQRL Schema

```yml
name: "orders"
schema_version: "1"
columns:
- name: "id"
  type: "INTEGER"
  tests:
  - "not_null"
- name: "customerid"
  type: "INTEGER"
  tests:
  - "not_null"
- name: "time"
  type: "DATETIME"
  tests:
  - "not_null"
- name: "items"
  cardinality:
    min: 1
    max: 1000
  columns:
  - name: "productid"
    type: "INTEGER"
    tests:
    - "not_null"
  - name: "quantity"
    type: "INTEGER"
    tests:
    - "not_null"
  - name: "unit_price"
    type: "FLOAT"
    tests:
    - "not_null"
  - name: "discount"
    type: "FLOAT"
  tests:
  - "not_null"
```

#### Schema Definition

DataSQRL schema supports the following attributes to define the data structure:

| Field Name     | Description                                                                                                                   | Required? |
|----------------|-------------------------------------------------------------------------------------------------------------------------------|-----------|
| name           | Name of the table that this schema applies to                                                                                 | Yes       |
| schema_version | Version of DataSQRL schema for this schema file                                                                               | Yes       |
| description    | Description of the table                                                                                                      | No        |

A table is defined by a list of columns. A column has a `name`. A column is either a scalar field or a nested table.

A column is defined by the following attributes:

| Field Name    | Description                                                        | Required?                           |
|---------------|--------------------------------------------------------------------|-------------------------------------|
| name          | Name of the column. Must be unique per table at any nesting level. | Yes                                 |
| description   | Description of the column                                          | No                                  |
| default_value | Value to use when column is missing in input data.                 | No                                  |
| type          | Type for a scalar field                                            | One of `type`, `columns` or `mixed` |
| columns       | Columns for a nested table                                         | One of `type`, `columns` or `mixed` |
| mixed         | A mix of scalar fields and nested tables for unstructured data     | One of `type`, `columns` or `mixed` |
| tests         | A set of constraints that the column satisfies                     | No                                  |

A column must either have a type (for scalar field) or a list of columns (for nested table). For unstructured data (i.e. data that does not conform to a schema), there is a third option to define a *mixed column* which can be a combination of multiple scalar fields or nested tables.

A mixed column is defined by the attribute `mixed` which is a map of multiple column definitions that are identified by a unique name.

```yml
- name: "time"
  mixed: 
    - "epoch":
        type: INTEGER
    - "timestamp":
        type: DATETIME
```

This defines the column `time` to be a mixed column that is either a scalar field called `epoch` with an `INTEGER` type or a scalar field called `timestap` with a `DATETIME` type. We would use such a mixed column definition for data where `time` is either represented as seconds since epoch or a timestamp.

Each individual column of a mixed column definition gets mapped onto a separate column in the resulting SQRL table with the column name being a combination of the mixed column name and the map key. For our example above, the SQRL `orders` table would contain a column `time_epoch` and `time_timestamp` for each of the respective scalar fields.

#### Scalar Types

DataSQRL schema supports these scalar types:

* **INTEGER**: for whole numbers
* **FLOAT**: for floating point numbers
* **BOOLEAN**: true or false
* **DATETIME**: point in time
* **STRING**: character sequence
* **UUID**: unique identifier
* **INTERVAL**: for periods of time

To define arrays of scalar types, wrap the type in square brackets. For instance, an integer array is defined as `[INTEGER]`.

#### Data Constraints

The `test` attribute specifies data constraints for columns, whether scalar field or nested table. These constraints are validated when data is ingested to filter out invalid or unneeded data. The constraints are also used to validate statements in SQRL scripts. In addition, the DataSQRL [optimizer](/docs/reference/sqrl/learn#datasqrl-optimizer) analyzes the constraints to build more efficient data pipelines.

DataSQRL schema supports the following test constraints:

* **not_null**: the column can not be missing or have a null value.
* **unique**: the column values are unique across all records in this table.


## Static Data

For development and testing, it is useful to use static data files as source data. To use static data as a source, convert the data to one of the following formats:

* [JSON Lines format](https://jsonlines.org/): A text file of newline seperated json documents that ends in `.jsonl`.
* [CSV Format](https://en.wikipedia.org/wiki/Comma-separated_values): A text file that contains comma-separated data and a header line. The file have the `.csv` extension.

Give the file the name of the table and place it in a local package folder. You can import the data without any additional configuration since DataSQRL will automatically infer the schema and create the schema and table configuration files in the package folder.

For example, if we place the file `MyTable.jsonl` in the folder `mypackage`, we can import the data with:
```sql
IMPORT mypackage.MyTable;
```

When you invoke the compiler, run or test your SQRL script, DataSQRL automatically discovers the schema and creates the table configuration and YAML schema files in the package/folder. You can then adjust both to suit your needs. To update the configuration and schema after making changes to the source data, delete those two files.