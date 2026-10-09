# Deep Dive: How DataSQRL Works

The DataSQRL `compile` command executes the following steps:

1. **Read Configuration**: Read the package.json configuration files and merge them on top of the built-in defaults to initialize the configuration for the compiler. The `run` and `test` commands add their own defaults.
2. **Build Project**: The [packager](#packager) builds the project structure in the `build/` directory.
3. **Plan Script**: The [parser](#parser) reads the main SQRL script in the `build/` directory and resolves all `IMPORT` and `EXPORT` statements locally against the folder structure in the `build/` directory. Statement by statement, the parser converts each statement to a logical plan, the [logical plan analyzer](#logical-plan-analyzer) validates it and extracts the information needed for planning, and the result is added to the processing DAG that defines the flow of data from sources to sinks.
4. **Optimize DAG**: The [DAG planner](#dag-planner) optimizes the DAG and assigns each table and function to a [stage](#architecture) for execution.
5. **Generate Physical Plans**: The [physical planner](#physical-planner) generates the deployment assets for each engine and the connector configuration to move data between engines.
6. **Generate API**: If the pipeline contains a server stage, the compiler infers or loads the GraphQL schema for each API version and generates the server model and the OpenAPI specification. For the `test` command, it also generates the test plan.
7. **Write Output**: The compilation outputs that describe the planned pipeline (e.g. `pipeline_explain.txt` and `pipeline_visual.html`) are written to the `build/` folder, and the deployment artifacts are written to the `build/deploy/plan` folder. Both locations can be changed with the `--build` and `--target` options. See [Compilation Output](compilation-output.md) for all files.

The DataSQRL `run` command executes all compilation steps above and:
1. **Launch**: Starts the engines the pipeline needs inside the DataSQRL container: Redpanda and PostgreSQL as local processes, and Flink and Vert.x embedded in the DataSQRL process. Redpanda is not started when an external Kafka cluster is configured through `KAFKA_BOOTSTRAP_SERVERS`.
2. **Deploy**: Deploys the deployment assets to the engines, e.g. installs the database schema, creates the Kafka topics, passes the server model to Vert.x, and executes the compiled plan (or the Flink SQL script) in Flink.
3. **Run**: Runs the engines as they execute the pipeline.

The running data pipeline and the individual engines running each component are accessible locally via the mapped ports. See the [run command](compiler.md#run-command) for details.

The DataSQRL `test` command executes all compilation and run steps above and:
1. **Subscriptions**: Registers the subscription queries to listen for test results (if any).
2. **Mutations**: Runs the mutation queries against the API in alphabetical order (if any) and snapshots the results.
3. **Await**: Waits for the configured interval, the number of checkpoints, or the Flink job completion based on configuration, then stops the Flink job.
4. **Queries**: Runs the queries in the test folder as well as generated queries for the API endpoints against the API and snapshots the results.
5. **Snapshots**: Snapshots all subscription results in string order and compares all snapshots against the expected ones. When new snapshots are created, the test is run once more to verify them.

See the [test command](compiler.md#test-command) for details.

The `exec` command runs the deployment artifacts of a previous compilation without compiling the project again. See the [exec command](compiler.md#exec-command).

## Architecture

DataSQRL supports a pluggable engine architecture. A data pipeline or microservice
consists of multiple stages and each stage is executed by an engine.
For example, a data pipeline may consist of a stream processing, storage, and
serving stage which are executed by Apache Flink, PostgreSQL, and Vert.x, respectively.

DataSQRL supports the following types of stages:

* Stream Processing: For processing data as it is ingested
  * [Apache Flink](https://flink.apache.org/)
* Log: For moving data between stages reliably
  * [Apache Kafka](https://kafka.apache.org/)
  * [Redpanda](https://www.redpanda.com/)
  * Apache Kafka-compatible (e.g. [Azure Event Hubs](https://azure.microsoft.com/en-us/products/event-hubs/))
* Database: For storing and querying data
  * [PostgreSQL](https://www.postgresql.org/)
  * [Apache Iceberg](https://iceberg.apache.org/) as the table format, queried by one of the query engines below
* Query: For querying data stored in Iceberg tables
  * [DuckDB](https://duckdb.org/)
  * [Snowflake](https://www.snowflake.com/)
  * [Trino](https://trino.io/)
  * [Apache Spark SQL](https://spark.apache.org/sql/)
  * [Amazon Redshift](https://aws.amazon.com/redshift/)
* Server: For returning data through GraphQL, REST, and MCP APIs upon request
  * [Vert.x](https://vertx.io/)
  * [GraphQL Java](https://www.graphql-java.com/)
* Export: For printing data to the console during development (`print`)

The engines that make up a pipeline are enabled with the `enabled-engines` configuration option.
Results retrieved from a table can be cached on the server with the `cache` hint (see [SQRL language](sqrl-language.md)).

Currently, DataSQRL is closely tied to Flink as the stream processing engine.
The other engines are modular, making it simple to add additional engines.

A data pipeline topology is a sequence of stages. A pipeline topology may contain
multiple stages of the same type (e.g. two different database stages).
An engine is what executes the deployment assets for a given stage.
For example, the Flink SQL generated by the compiler as part of the deployment assets for the "stream" stage is
executed by the Flink engine.

The pipeline topology as well as other compiler configuration options are
specified in a JSON configuration file typically called `package.json`.
The [configuration documentation](configuration.md) lists all the configuration options.

## Planner Components

The planner parses a SQRL script, i.e. a sequence of SQL statements, analyzes
the statements, constructs a data processing DAG, optimizes the DAG, and finally
produces deployment assets for the engines executing the data processing steps.

The planner consists of the following components.

### Packager

The packager populates the `build/` directory with the files of the local project and the packages included through the `script.include` configuration option, each under its own namespace.
It writes the merged configuration to `build/package.json`.

As part of this process, the packager executes the following preprocessors:

* The SQRL preprocessor renders template variables in `.sqrl` scripts with the values configured under `script.config`.
* The UDF preprocessors extract user-defined function definitions from provided JAR files and from [JBang](https://www.jbang.dev/) `.java` files (see [functions](functions.md)).
* The static data preprocessor copies `.jsonl`, `.csv`, and `.avro` files (optionally compressed) into a consolidated data directory for Flink to read at runtime. This requires that filenames for static data files are unique.

Preprocessors are internal to DataSQRL and can be extended within the framework.

The schema of `.jsonl` and `.csv` data files is discovered during planning when a table definition references the file.

### Parser

The parser is the first stage of the compiler. The parser parses the
SQRL script into a logical plan by pre-processing any SQRL specific syntax and then
passing the result to the Flink SQL parser to produce a logical plan.

The parser resolves imports against the build directory using module loaders
that retrieve dependencies. It maintains a schema of all defined tables
in a SQRL script.

The parser is built on top of Apache Calcite by way of Flink SQL for all SQL handling.
It prepares the statements that are analyzed and planned by the planner.

### Logical Plan Analyzer

The logical plan analyzer is the second stage of the compiler. It takes the
logical plan produced by the parser for each table or function
defined in the SQRL script and analyzes the logical plan to extract a TableAnalysis
that contains information needed by the planner.

1. It keeps track of important metadata like timestamps, primary keys, sort orders, table types, hints, and the capabilities an engine requires to execute the statement.
2. It analyzes the SQL to identify potential issues, semantic inconsistencies, or optimization potential and produces warnings or notices.
3. It extracts cost information for the optimizer.

### DAG Planner

The DAG planner takes all the individual table and function definitions and assembles them into
a data processing DAG (directed acyclic graph). It prunes the DAG and rewrites the DAG before optimizing the
DAG to assign each node (i.e. table or function) to a stage in the pipeline.

The optimizer uses a cost model and is constrained to produce only viable
pipelines.

At the end of the DAG planning process, each table or function defined in the SQRL script
is assigned to a stage in the pipeline.

### Physical Planner

All the tables in a given stage are then passed to the stage engine's physical
planner which produces the physical plan for the engine that has been
configured to execute that stage.

The physical plan assets produced depend on the engine:
* Apache Flink: A Flink SQL script and compiled plan which contains the generated connector configuration
* PostgreSQL: A SQL schema for the tables and index structures as well as view definitions
* Iceberg: A SQL schema for the tables as well as table and view definitions for the configured query engine
* Kafka: A list of topics with configuration
* Vert.x: A server model with the GraphQL schema and query execution plan, the server configuration, and an OpenAPI specification

See [Compilation Output](compilation-output.md) for a complete list of the generated files.

Physical planning can contain additional optimization such as selecting optimal index
structures for database tables.

An important step in generating the physical plan is generating the connector configuration between engines.
When two adjacent nodes in the DAG are assigned to different engines for execution,
we consider this a "cut" in the DAG since it cuts the DAG into multiple sub-graphs -- one for each engine.
To move data between engines at the cut points (i.e. the edges that connect the respective nodes), a connection needs to be established.

Connector configuration is generated for the stream processing engine (Apache Flink)
to read from and write to the database, filesystem, streaming platform, etc.
The connector configuration is determined by the physical planner based on the logical plan analysis above
and instantiated in connector templates that are configured in the [configuration file](configuration.md).
The server engine (Vert.x) connects to the databases and the streaming platform through its server configuration.

The physical planner is also responsible for generating the API schemas (e.g. GraphQL schema)
for the exposed API if the pipeline contains a server engine. Optionally, the user may
provide the API schema in which case the physical planner validates the schema and maps it
to the SQRL script.

The physical plans are then written out as deployment artifacts to the `build/deploy/plan`
directory.
