# DataSQRL Configuration

You control the topology, execution characteristics, and API of the DataSQRL generated data pipeline/microservice through configuration files.

* **Package.json:** Controls all aspects of the compiler.
* **GraphQL Schema:** Specifies the resulting GraphQL API.

## Package.json

The package configuration is the central configuration file used by DataSQRL. The package configuration declares dependencies, configures the engines in the data pipeline, sets compiler options, and provides package information.

You can pass a configuration file to the compiler via the `-c` or `--config` flag. You can specify a single configuration file or multiple files. If multiple files are specified, they are merged in the order they are specified (i.e. fields - including arrays - are replaced and objects are merged).  

DataSQRL allows environment variables: `${VAR}`.

A minimal package json contains the following:
```json
{
  "version": "1"
} 
```

### Engines

`engines` is a map of engine configurations by engine name that the compiler uses to instantiate the engines in the data pipeline. The DataSQRL compiler produces an integrated data pipeline against those engines. At a minimum, DataSQRL expects that a stream processing engine is configured.

Engines can be enabled with the enabled-engines property. The default set of engines are listed below:
```json
{
  "enabled-engines": ["vertx", "postgres", "kafka", "flink"]
}
```

#### Flink

Apache Flink is the default stream processing engine. 

The physical plan that DataSQRL generates for the Flink engine includes:
* FlinkSQL table descriptors for the sources and sinks
* FlinkSQL view definitions for the data processing
* A list of connector dependencies needed for the sources and sinks.

Flink reads data from and writes data to the engines in the generated data pipeline. DataSQRL uses connector configuration templates to instantiate those connections.
These templates are configured under the `connectors` property.

Connectors that link flink to other engines and external systems can be configured in the `connectors` property. Connectors use the [flink configuration options](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/overview/) and are directly passed through to flink without modification.

Environment variables that start with the `sqrl` prefix are templated variables that the DataSQRL compiler instantiates. For example: `${sqrl:table}` provides the table name for a connector that writes to a table.

```json
{
  "engines" : {
    "flink" : {
      "connectors": {
        "postgres": {
          "connector": "jdbc",
          "password": "${JDBC_PASSWORD}",
          "driver": "org.postgresql.Driver",
          "username": "${JDBC_USERNAME}",
          "url": "${JDBC_URL}",
          "table-name": "${sqrl:table}"
        },
        "kafka": {
          "connector" : "kafka",
          "format" : "flexible-json",
          "properties.bootstrap.servers": "${PROPERTIES_BOOTSTRAP_SERVERS}",
          "properties.group.id": "${PROPERTIES_GROUP_ID}",
          "scan.startup.mode" : "group-offsets",
          "properties.auto.offset.reset" : "earliest",
          "topic" : "${sqrl:topic}"
        },
        "iceberg" : {
          "warehouse":"s3://daniel-iceberg-table-test",
          "catalog-impl":"org.apache.iceberg.aws.glue.GlueCatalog",
          "io-impl":"org.apache.iceberg.aws.s3.S3FileIO",
          "catalog-name": "mydatabase"
        }
      }
    }
  }
}
```

Flink runtime configuration can be specified in the [`values` configuration](#values) section.


#### Postgres

Postgres is the default database engine.

The physical plan that DataSQRL generates for the Postgres engine includes:
* Table DDL statements for the physical tables.
* Index DDL statements for the index structures on those tables.
* View DDL statements for the logical tables. Views are only created when no server engine is enabled.

#### Vertx

Vertx is the default server engine. A high performance GraphQL server implemented in [Vertx](https://vertx.io/). The GraphQL endpoint is configured through the [GraphQL Schema](#graphql-schema).

The physical plan that DataSQRL generates for Vertx includes:
* The connection configuration for the database(s) and log engine
* A mapping of GraphQL endpoints to queries for execution against the database(s) and log engine.


#### Kafka

Apache Kafka is the default `log` engine.

The physical plan that DataSQRL generates for Kafka includes:
* A list of topics with configuration and (optional) Avro schema.

#### Iceberg

Apache Iceberg is a table format that can be used as a database engine with DataSQRL.

The `iceberg` engine requires an enabled query engine to execute queries against it.

The physical plan that DataSQRL generates for Kafka includes:
* Table DDL statements for the physical tables
* Catalog registration for registering the tables in the associated catalog, e.g. AWS Glue.

#### Snowflake

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

### Compiler

The `compiler` section of the configuration controls elements of the core compiler and DAG Planner. 

```json
{
  "compiler" : {
    "addArguments": true,
    "logger": "print",
    "explain": {
      "visual": true,
      "text": true,
      "extended": false
    }
  }
}
```

* `addArguments` specifies whether to include table columns as filters in the generated GraphQL schema. This only applies if the GraphQL schema is generated by the compiler.
* `logger` configures the logging framework used for logging statements like `EXPORT MyTable TO logger.MyTable;`. It is `print` by default which logs to STDOUT. Set it to the configured log engine for logging output to be sent to that engine, e.g. `"logger": "kafka"`. Set it to `none` to suppress logging output.
* `explain` configures how the DAG plan compiled by DataSQRL is presented in the `build` directory. If `visual` is true, a visual representation of the DAG is written to the `pipeline_visual.html` file which you can open in any browser. If `text` is true, a textual representation of the DAG is written to the `pipeline_explain.txt` file. If `extended` is true, the DAG outputs include more information like the relational plan which may be very verbose.


### Profiles

```json
{
  "profiles": ["myprofile"]
}
```

The deployment profile determines the deployment assets that are generated by the DataSQRL compiler. For example, the default profile generates docker images and a docker compose template for orchestrating the entire pipeline compiled by DataSQRL.

You can configure a single or multiple deployment profiles which are merged together.

A deployment profile can also be downloaded from the repository when it's fully qualified package name is specified:

```json
{
  "profiles" : ["datasqrl.profile.default"]
}
```

Learn more about [deployment profiles](../deployments).

### Values

The `values` section of the [DataSQRL configuration](../datasqrl-spec) allows you to specify configuration values that are passed through to the deployment profile and can be referenced in the deployment profile templates. See [deployment profiles](../deployments) for more information.

The default deployment profiles supports a `flink-config` section to allow injecting additional flink runtime configuration. You can use this section of the configuration to specify any [Flink configuration option](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/config/).

```json
{
 "values" : {
    "flink-config" : {
      "taskmanager.memory.network.max": "800m",
      "execution.checkpointing.mode" : "EXACTLY_ONCE",
      "execution.checkpointing.interval" : "1000ms"
    }
   }
}
```

The `values` configuration settings take precedence over identical configuration settings in the compiled physical plans.

### Dependencies

`dependencies` is a map of all packages that a script depends on. The name of the dependency is the key in the map and the associated value defines the dependency by `version` and `variant`.

While explicit package dependencies are encouraged, DataSQRL will automatically look up packages in the SQRL script in the [repository](https://dev.datasqrl.com).

```json
{
  "dependencies" : {
    "datasqrl.seedshop" : {
      "name": "datasqrl.seedshop",
      "version" : "0.1.0",
      "variant" : "dev"
    }
  }
}
```

This example declares a single dependency `datasqrl.seedshop`. The DataSQRL packager retrieves the `datasqrl.seedshop` package from the repository for the given version "0.1.0" and "dev" variant and makes it available for the compiler. The `variant` is optional and defaults to `default`. <br />
Note, that specifying the name in this case is optional. You have to specify the name if the package name in the SQRL script is different from the name in the repository (i.e. you want to rename the package).

**Variants**
Package can have multiple variants for a given version. A variant might be a subset, static snapshot, or point to an alternate data system for development and testing. 

**Dependency Aliasing**:

We can also rename dependencies which makes it easy to dynamically swap out dependencies for different environments and testing.
```json
{
  "dependencies":
  {
    "datasqrl.tutorials.seedshop": { 
      "name": "local-seedshop", 
      "version":  "1.0.0", 
      "variant":  "dev" }
  }
}
```
In the above example, the `local-seedshop` directory will be looked up and renamed to `datasqrl.tutorials.seedshop`.

### Script

The main SQRL script and GraphQL schema for the project can be configured in the project configuration under the `script` section:

```json
 {
  "script": {
    "main": "mainScript.sqrl",
    "graphql": "apiSchema.graphqls"
  }
}
```


### Package Information

The `package` section of the configuration provides information about the package or script. The whole section can be omitted when compiling or running a script. It is required when publishing a package to the repository.

:::info
To publish a package to the remote repository, the first component of the package name path has to match your DataSQRL account name or the name of an organization you are part of.
:::

Learn more about publishing in the CLI documentation.

```json
{
  "package": {
    "name": "datasqrl.tutorials.Quickstart",
    "version": "0.0.1",
    "variant": "dev",
    "description": "A docker compose datasqrl profile",
    "homepage": "https://www.datasqrl.com/docs/getting-started/quickstart",
    "documentation": "Quickstart tutorial for datasqrl.com",
    "topics": "tutorial"
  }
}
```

| Field Name    | Description                                                                                                                                                                   | Required? |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| name          | Name of the package. The package name should start with the name of the individual or organization that provides the package.                                                 | Yes       |
| version       | The version of the package. We recommend to use [semantic versioning](https://semver.org/).                                                                                   | Yes       |
| variant       | The variant of the package if multiple variants are available. Defaults to `default`.                                                                                         | No        |
| latest        | If this is the latest version of this package. DataSQRL uses the latest version when looking up missing packages on import. Defaults to `false`. | No        |
| description   | A description of the package.                                                                                                                                                 | No        |
| license       | The license used by this package.                                                                                                                                             | No        |
| documentation | Link that points to documentation for this package                                                                                                                            | No        |
| homepage      | Link that points to the homepage for this package                                                                                                                             | No        |


### Testing

Testing related configuration is found in the `test-runner` section.

```json
{
  "test-runner": {
    "delay-sec": 30
  }
}
```

* `delay-sec`: The number of seconds to wait between starting the processing of data and snapshotting the data.


## GraphQL Schema


<!--
**SQRL Integration**

(in development) Graphql mutations and subscriptions automatically create log sources and sinks in SQRL. In addition, a special variable is given to SQRL called `@JWT` which can access jwt token information.
```sql
MyUser(@JWT.user: String) := SELECT * FROM Users WHERE id = @JWT.user;
```
-->

