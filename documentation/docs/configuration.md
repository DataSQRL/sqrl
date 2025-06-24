
# DataSQRL Configuration

The data pipeline topology, compiler options, dependencies, connector templates and more are configured through
the DataSQRL package.json configuration file.

You pass a configuration file to the [compiler command](compiler) via the `-c` or `--config` flag. You can specify a single configuration file or multiple files. If multiple files are specified, they are merged in the order they are specified (i.e. fields - including arrays - are replaced and objects are merged). If now configuration file is explicitly specified, the compiler will use the `package.json` file in the local directory if it exists or use the default configuration.

## Engines

DataSQRL compiles SQL scripts to the engines that execute the resulting data pipeline. DataSQRL supports different types of engines like stream processing (Apache Flink), stream platform (Apache Kafka, RedPanda, etc), databases (Postgres, DuckDB), table formats (Apache Iceberg), and servers (Vert.x). 

`engines` is a map of engine configurations by engine name that the compiler uses to instantiate the engines in the data pipeline. The DataSQRL compiler produces an integrated data pipeline against those engines. At a minimum, DataSQRL expects that a stream processing engine is configured.

Engines can be enabled with the enabled-engines property. The default set of engines are listed below:
```json
{
  "enabled-engines": ["vertx", "postgres", "kafka", "flink"]
}
```

### Flink

Apache Flink is the default stream and batch processing engine. The execution mode is controlled by the `mode` configuration option and can either be `streaming` (the default) or `batch`.

The physical plan that DataSQRL generates for the Flink engine includes:
* FlinkSQL table descriptors for the sources and sinks
* FlinkSQL view definitions for the data processing
* A list of connector dependencies needed for the sources and sinks.
* A compiled plan
* An explained plan

Flink reads data from and writes data to the engines in the generated data pipeline. DataSQRL uses connector configuration templates to instantiate those connections.
These templates are configured under the `connectors` property.

Connectors that connect flink to other engines and external systems can be configured in the `connectors` property. Connectors use the [flink configuration options](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/overview/) and are directly passed through to flink without modification.

Environment variables that start with the `sqrl` prefix are templated variables that the DataSQRL compiler instantiates. For example: `${sqrl:table-name}` provides the table name for a connector that writes to a table.

Flink configuration can be specified in the `config` section. See [Flink configuration](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/config/) for details.
These configuration options are applied when compiling the Flink plan and when executing Flink while running or testing via the [DataSQRL command](compiler). 


```json
{
  "engines" : {
    "flink" : {
      "mode" : "streaming",
      "connectors": {
        "postgres": {
          "connector": "jdbc-sqrl",
          "password": "${JDBC_PASSWORD}",
          "driver": "org.postgresql.Driver",
          "username": "${JDBC_USERNAME}",
          "url": "jdbc:postgresql://${JDBC_URL}",
          "table-name": "${sqrl:table-name}"
        },
        "kafka": {
          "connector" : "kafka",
          "format" : "flexible-json",
          "properties.bootstrap.servers": "${PROPERTIES_BOOTSTRAP_SERVERS}",
          "properties.group.id": "${PROPERTIES_GROUP_ID}",
          "scan.startup.mode" : "group-offsets",
          "properties.auto.offset.reset" : "earliest",
          "topic" : "${sqrl:original-table-name}"
        }
      },
      "config" : {
        "table.exec.source.idle-timeout": "5000 ms"
      }
    }
  }
}
```

### Postgres

Postgres is the default database engine.

The physical plan that DataSQRL generates for the Postgres engine includes:
* Table DDL statements for the physical tables.
* Index DDL statements for the index structures on those tables.
* View DDL statements for the logical tables. Views are only created when no server engine is enabled.

### Vertx

Vertx is the default server engine. A high performance GraphQL server implemented in [Vertx](https://vertx.io/). The GraphQL endpoint is configured through the [GraphQL Schema](#graphql-schema).

The physical plan that DataSQRL generates for Vertx includes:
* The connection configuration for the database(s) and log engine
* A mapping of GraphQL endpoints to queries for execution against the database(s) and log engine.


### Kafka

Apache Kafka is the default `log` engine.

The physical plan that DataSQRL generates for Kafka includes:
* A list of topics with configuration and (optional) Avro schema.

### Iceberg

Apache Iceberg is a table format that can be used as a database engine with DataSQRL.

The `iceberg` engine requires an enabled query engine to execute queries against it.

The physical plan that DataSQRL generates for Kafka includes:
* Table DDL statements for the physical tables
* Catalog registration for registering the tables in the associated catalog, e.g. AWS Glue.

### Snowflake

Snowflake is a query engine that can be used in combination with a table format as a database in DataSQRL.

The physical plan that DataSQRL generates for Kafka includes:
* External table registration through catalog integration. The Snowflake connector currently support AWS Glue.
* View definitions for the logical tables.

To define the catalog integration for Snowflake:
```json
{
  "snowflake" : {
    "catalog-name": "MyCatalog",
    "external-volume": "iceberg_storage_vol"
  }
}
```

## Compiler

The `compiler` section of the configuration controls elements of the core compiler and DAG Planner.

```json
{
  "compiler" : {
    "logger": "print",
    "explain": {
      "visual": true,
      "text": true,
      "extended": false
    },
    "compilePlan": true
  }
}
```

* `logger` configures the logging framework used for logging statements like `EXPORT MyTable TO logger.MyTable;`. It is `print` by default which logs to STDOUT. Set it to the configured log engine for logging output to be sent to that engine, e.g. `"logger": "kafka"`. Set it to `none` to suppress logging output.
* `explain` configures how the DAG plan compiled by DataSQRL is presented in the `build` directory. If `visual` is true, a visual representation of the DAG is written to the `pipeline_visual.html` file which you can open in any browser. If `text` is true, a textual representation of the DAG is written to the `pipeline_explain.txt` file. If `extended` is true, the DAG outputs include more information like the relational plan which may be very verbose.
* `compilePlan` configures whether the compiler attempts to compile the individual SQL plans for each engine into executable plans (i.e. physical plans) for deterministic execution. This only applies to engines and configurations that support plan compilation (currently, that's only Flink in streaming mode).

## Dependencies

`dependencies` map import and export paths to local folders or remote repositories

**Dependency Aliasing**:

Dependency declarations can be used to alias a local folder:

```json
{
  "dependencies" : {
    "datasqrl.seedshop" : {
      "name": "datasqrl.seedshop.test"
    }
  }
}
```

When we import `IMPORT datasqrl.seedshop.Orders` the `datasqrl.seedshop` path is aliased to the local folder `datasqrl/seedshop/test`.

This is useful for swapping out connectors between different environments or for testing without making changes to the SQRL script.

<!--
**Repository Imports**:

Dependencies can be used to import tables or functions from a remote repository:

```json
{
  "dependencies" : {
    "sqrl-functions" : {
      "name": "sqrl-functions",
      "repository": "github.com/DataSQRL/sqrl-functions",
      "tag": "v0.6.0"
    }
  }
}
```

This dependency configuration clones the referenced repository at the tag `v0.6.0` into the folder `build/sqrl-functions`.
We can then import the openai functions as:

```sql
IMPORT sqrl-functions.openai.vector_embedding;
```

If the tag is omitted, it clones the current main branch.
-->
## Script

The main SQRL script and (optional) GraphQL schema for the project can be configured in the project configuration under the `script` section:

```json
 {
  "script": {
    "main": "mainScript.sqrl",
    "graphql": "apiSchema.graphqls"
  }
}
```

If the script is configured in the configuration it is not necessary to name it as a command argument. Script arguments take precedence over the configured value.


## Package Information

The `package` section of the configuration provides information about the package or script. The whole section is optional and used primarily when hosting packages in a repository as dependencies.

```json
{
  "package": {
    "name": "datasqrl.tutorials.Quickstart",
    "description": "A docker compose datasqrl profile",
    "homepage": "https://www.datasqrl.com/docs/getting-started/quickstart",
    "documentation": "Quickstart tutorial for datasqrl.com",
    "topics": ["tutorial"]
  }
}
```

| Field Name    | Description                                                                                                                   | Required? |
|---------------|-------------------------------------------------------------------------------------------------------------------------------|-----------|
| name          | Name of the package. The package name should start with the name of the individual or organization that provides the package. | Yes       |
| description   | A description of the package.                                                                                                 | No        |
| license       | The license used by this package.                                                                                             | No        |
| documentation | Link that points to documentation for this package                                                                            | No        |
| homepage      | Link that points to the homepage for this package                                                                             | No        |
| homepage      | An array of keywords or topics that label the contents of the package                                                         | No        |


## Testing

Testing related configuration is found in the `test-runner` section.

```json
{
  "test-runner": {
    "delay-sec": 30,
    "mutation-delay": 1
  }
}
```

* `delay-sec`: The number of seconds to wait between starting the processing of data and snapshotting the data. Defaults to 30.
* `required-checkpoints`: The number of checkpoints that need to be completed before the test queries are executed. Defaults to 0. If this is configured to a positive value, `delay-sec` must be set to -1.
* `mutation-delay`: The number of seconds to wait between execution mutation queries. Defaults to 0.


## Values

The `values` section of the configuration allows you to specify configuration values that is applied at runtime when running or testing SQRL scripts with the [DataSQRL command](compiler).

```json
{
 "values" : {
    "create-topics": ["mytopic"]
   }
}
```

For the log engine, the `create-topics` option allows you to specify topics to create in the cluster prior to starting the pipeline. This is useful for testing.

## Environmental Variables

To reference an environmental variable in the configuration, use the standard environmental variable syntax `${VAR}`.

