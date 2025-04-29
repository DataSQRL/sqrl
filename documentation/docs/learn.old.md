
# DataSQRL

## Introduction
DataSQRL enhances the SQRL language to optimize data pipeline construction for developers. It features an advanced optimizer that efficiently directs queries across multiple integrated engines. This flexible, composable architecture allows for customization and scalability by enabling the integration of only the necessary engines. DataSQRL also offers comprehensive developer tools, including schema management, generating deployment assets, a repository for dependency management, efficient dependency handling, and more. These features make DataSQRL a versatile and powerful tool for developing and managing data pipelines.

DataSQRL is an open-source project, which means you can view the [entire source code](https://github.com/DataSQRL/sqrl), [fork](https://github.com/DataSQRL/sqrl/fork) and customize it, and make contributions to the project.

<img src="/img/index/undraw_contribute_sqrl.svg" alt="Developer Documentation >" width="40%"/>

### Mission and Goals behind DataSQRL
The fundamental mission of DataSQRL is to democratize the process of building efficient, scalable data products by making advanced data pipeline tools accessible and easy to use. Our goal is to empower developers by simplifying the data pipeline construction process, reducing the barrier to entry, and accelerating the path from development to production.

Key Objectives:
- **Empowering Simplicity**: We prioritize making DataSQRL intuitive and straightforward for a wide range of use cases, focusing on delivering functionality that caters to the core needs of most projects. While this means some specialized features may not be included, our aim is to cover the broadest possible spectrum of data pipeline needs efficiently.

- **Flexibility in Integration**: DataSQRL's architecture is designed to be composable and modular, allowing developers to integrate only the components they need. This approach facilitates seamless integration with existing data ecosystems and enables scalability tailored to specific project demands.

- **Innovative Optimization**: At the heart of DataSQRL lies an advanced optimizer capable of intelligently splitting queries across multiple integrated engines. This feature ensures optimal performance and resource utilization, making it possible to handle complex data operations smoothly.

- **Comprehensive Developer Support**: DataSQRL is equipped with robust developer tools that streamline various aspects of data pipeline management—from schema evolution and dependency management to the generation of deployment assets. These tools are designed to boost productivity and enhance the overall developer experience.

- **Community and Collaboration**: As an open-source project, DataSQRL thrives on community input and collaboration. We are committed to maintaining an active dialogue with our users and continuously evolving the tool based on feedback and emerging data technology trends.

### Relationship to SQRL Language Specification
DataSQRL builds upon the SQRL language specification, which defines syntax and semantics for defining full data pipelines, by adding practical execution and optimization capabilities. While the SQRL specification outlines what is syntactically correct and the basic semantic understanding of operations, it does not address execution across different systems, developer features, or optimization within complex architectures.

**Key Enhancements in DataSQRL**:
- **Engine Translation**: DataSQRL extends SQRL by translating queries to be compatible with various integrated engines, enhancing the execution efficiency across diverse technological environments.

- **Advanced Optimization**: DataSQRL introduces an optimization layer that smartly allocates queries across resources, selecting the optimal engine for each task and refining execution plans to boost performance and reduce resource use.

- **Development Tools**: Beyond the basic language, DataSQRL offers tools for schema management, deployment automation, and dependency management, supporting the entire lifecycle of data pipelines.

- **Complex Data Operations**: DataSQRL enhances SQRL’s handling of real-world data processing challenges, including streaming data, time-based computations, and complex data transformations necessary for in-depth data pipeline integration.

## Architecture Overview
<!-- Detailed explanation of DataSQRL architecture: Modular engine architecture, compiler, optimizer, tooling, image of architecture, etc -->
DataSQRL’s architecture is designed to efficiently handle and process data through a series of integrated components, each serving a specific function in the data pipeline lifecycle. Here’s a straightforward look at each of these components and how they contribute to the overall system:

<img src="/img/dev/compilation.svg" alt="DataSQRL compilation overview >" width="400"/>

<!--
* **SQRL Scripts**: SQRL scripts are text files that implement the logic of the data product in the [SQRL language](/docs/reference/sqrl/overview) which is a SQL dialect. SQRL scripts can import tables defined in other scripts which are resolved and compiled recursively.
* **API Specification** *(optional)*: The [specification of the API](/docs/reference/api/overview) that the DataSQRL compiler generates. Currently, DataSQRL supports GraphQL schema with OpenAPI (Rest) and protocol buffers (gRPC) on the [roadmap](../roadmap). If no API specification is provided, DataSQRL will generate an API that exposes all visible tables and columns defined in the SQRL script.
* **Package Configuration** *(optional)*: The `package.json` file in the directory where the DataSQRL compiler is invoked is the package configuration which defines all configuration options and declares all dependencies. The package configuration defines the architecture of the data pipeline that DataSQRL compiles to and configures all engines in that architecture. If no package manifest is present, DataSQRL uses a default topology, default engines (Flink, Postgres, and Vertx), and default configuration options.
* **Source and Sink Definitions**: The packages referenced by `IMPORT` and `EXPORT` statements in the SQRL script are resolved to data sources and sinks, respectively, which are either defined in local subdirectories or dependencies declared in the package configuration. If the package manager cannot resolve a dependency, it attempts to look it up in the [DataSQRL repository](/docs/reference/operations/repository).

Please refer to the `README.md` and package level documentation in the [repository](https://github.com/DataSQRL/sqrl) and modules for more information. Our aim is to keep most of the code documentation with the source code to avoid discrepancies.
-->
### Source Code
If you are interested to dive into the DataSQRL source code, here are some pointers to get you started:

* Most of the developer documentation lives with the source code. Check out the README files for each of the modules and the JavaDoc for package and class level documentation.

## Time Handling in SQRL
<!-- Detailed explanation of handling time and timestamps in scripts. How to use now() and other time-based functions effectively. -->
Time is an important concept in DataSQRL because it determines how data streams are processed.

For stream processing, it is useful to think of time as a timeline that stretches from the past through the present and into the future. The blue line in the picture below visualizes a timeline.

<img src="/img/reference/timeline.svg" alt="SQRL Timeline" width="100%" />

The blue vertical bar on the timeline represents the present. The present moment continuously advances on the timeline.

Each stream record processed by the system (shown as grey circles) is associated with a point on the timeline before the present. We call this point in time the **timestamp** of a stream record. The timestamp can be the time when an event occurred, a metric was observed, or a change happened.

### Now {#now}

SQRL uses the term **now** to designate the point in time to which the data pipeline has caught up in processing stream records. Now is the present moment from the perspective of the data pipeline. Now is marked as the orange vertical bar on the timeline.

Now is always behind the present. Now monotonically advances like the present, but it may not advance smoothly. If the data pipeline is operating with low latency, now can be just a few milliseconds behind the present. If stream records arrive with delay or the data pipeline is under a lot of load, now can be multiple seconds behind the present. And if the data pipeline crashes and restarts, now may fall minutes or hours behind the present and then catches back up as stream records are processed.

Now determines how time-based computations are executed. For example, when aggregating stream tables by time window, now determines when the time window closes.
```sql
Users.spending := SELECT endOfWeek(p.time) AS week,
         sum(t.price) AS spend, sum(t.saving) AS saved
      FROM @.purchases p JOIN p.totals t
      GROUP BY week ORDER BY week DESC;
```
The nested `spending` table aggregates users' orders by week and produces a stream table that contains one record per user per week with the weekly aggregates. That record is produced at the end of the week. The end of the week is determined by now and not the present time.

SQRL provides the function `now()` to refer to now in SQRL scripts.
```sql
Users.spending_last_week := SELECT sum(i.total) AS spend, 
            sum(i.discount) AS saved
      FROM @.purchases p JOIN p.items i
      WHERE p.time > now() - INTERVAL 7 DAY;
```
The nested `spending_last_week` table aggregates users' orders for the last week. It produces a state table since the aggregate changes as now advances, i.e. as older orders drop out of the aggregate and newer orders are added.

:::info
To summarize, use `now()` for recency comparisons and to refer to the present time in the data pipeline.
:::

Note, that `now()` is different from the standard SQL function `CURRENT_TIMESTAMP` or database specific current-time functions like `now()` in MySQL. These SQL function return the current system time of the system that is executing the function. `now()` in SQRL returns the time to which the data pipeline has caught up in processing stream records.

### Determining Timestamps

The timestamp of a stream table determines how stream records are associated with a point on the timeline and how now advances in the data pipeline.

For stream tables that are imported from a data source, the timestamp is configured explicitly in the [source configuration](/docs/reference/sqrl/datasqrl-spec#tablejson).

### Time Synchronization

The DataSQRL compiler synchronizes time between the components and systems of the data pipeline to ensure that all systems agree on now. However, millisecond imprecisions can arise at system boundaries due to communication overhead.

:::note
Time synchronization between the stream engine and database engine is not yet implemented. Database engines
use the system time to represent now which can lead to inaccuracies if now has fallen far behind the present moment or the system time is configured incorrectly.
:::

## Sources/Sinks
In DataSQRL, sources and sinks represent the endpoints of the data pipeline, integrating external data systems for both input and output operations. Configuration files define these connections, specifying how data is read from sources or written to sinks.

Data sources and sinks are defined in configuration files that are contained in packages. The configuration specifies how to connect to and read from (or write to) the source (or sink).

DataSQRL supports a lot of different data systems as sources and sinks, including Kafka, file system, object storage, Iceberg, Postgres, etc. Check out the [connectors](/docs/reference/sqrl/datasqrl-spec#tablejson) for all the data systems that DataSQRL can connect to.

When you are first getting started on a new project with DataSQRL, the easiest way to add a data source is to export your data (or a subset) to a [JSON Lines](https://jsonlines.org/) (i.e. line delimited json) or CSV files.

1. Place those files in a sub-folder at the root of your project. Let's say the folder is called `mySources`.
2. Import the data into your script via `IMPORT mySources.myDataFile;`

DataSQRL can automatically derive the configuration file and schema from static JSONL or CSV files. This makes it really easy to get started writing SQRL scripts without having to configure data connectors and figuring out how to access the source data systems.

Once you are ready to move to "real" data source connectors the files you used in the beginning remain useful for test cases.

## DataSQRL Optimizer {#datasqrl-optimizer}
The optimizer is part of the DataSQRL compiler and determines the optimal data pipeline architecture to execute a SQRL script. The DataSQRL optimizer runs a global optimization for the entire data pipeline and local optimizations for each individual engine that is part of the data pipeline architecture.

### Global Optimization

The DataSQRL compiler produces a computation DAG (directed, acyclic graph) of all the tables defined in the SQRL script and the result sets computed from those tables that are accessible.

The global optimizer determines which engine executes the computation of which table in the DAG.

<img src="/img/reference/reactive_microservice.svg" alt="DataSQRL data pipeline architecture >" width="50%"/>

For example, suppose we are compiling a SQRL script against the data pipeline architecture shown to the left, which consist of the Flink stream processor, a database, API server, and Kafka log in a circle that visualizes the data flow of the data pipeline. <br />
If we precompute a table in the stream engine, those results are readily available at request time which leads to fast response times and good API performance compared to having to compute the results in the database. However, pre-computing all possible results for the API can be very wasteful or outright impossible due to the number of possible query combinations.

The global compiler strives to find the right balance between pre-computing tables for high performance and computing results at request time to reduce waste in order to build efficient data pipelines.

In addition, the global optimizer picks the engine most suitable to compute each table of the global DAG and prunes the DAG to eliminate redundant computations.

### Local Optimization

The local optimizer takes the physical execution plans for each engine and runs them through an engine specific optimizer.

Local optimizers that are executed by DataSQRL include:

* **DAG Optimization:** Consolidates repeated computations in the stream processing DAG.
* **Index Selection:** Chooses an optimal set of indices for database engines to speed up queries executed for individual API calls.

### Optimizer Hints {#hints}

Sometimes the optimizer makes the wrong decision and produces sub-optimal data pipelines. You can provide hints in the SQRL script to correct those errors by overwriting the optimizer.

#### Execution Engine Hint

You annotate a table definition in SQRL with the name of an execution engine as a hint to tell the optimizer which engine should compute the table.

```sql
/*+ EXEC(streams) */
OrdersByMonth := SELECT endOfmonth(p.time) AS month,
              count(1) as num_orders
         FROM Orders GROUP BY month;
```

The annotation `EXEC(streams)` instructs the optimizer to compute the `OrdersByMonth` table in the `stream` engine. An engine with the name `stream` must be configured in the engines section of the [package configuration](/docs/reference/sqrl/datasqrl-spec).

Similarly, the `EXEC(database)` annotation instructs the optimizer to choose the engine with the name `database`:

```sql
/*+ EXEC(database) */
OrdersByMonth := SELECT endOfmonth(p.time) AS month,
              count(1) as num_orders
         FROM Orders GROUP BY month;
```

## Overview of Integrated Engines
An **engine** is a system or technology that executes part of the data pipeline compiled by DataSQRL.

Which engines DataSQRL compiles to is configured in the [package configuration](/docs/reference/sqrl/datasqrl-spec) which also defines the data pipeline architecture.

DataSQRL supports 4 types of engines that play distinct roles in a data pipeline: stream engines, database engines, server engines, log engines, query engines.

### Stream Engine
A stream engine is a stream processing system that can ingest data from external data sources, process the data, and pass the results to external data sinks.

DataSQRL currently supports the following stream engines:

* [Apache Flink](https://flink.apache.org/): Apache Flink is a fault-tolerant and scalable open-source stream processing engine.

#### Flink Stream Engine

[Apache Flink](https://flink.apache.org/) is an open-source stream processing engine designed to process large volumes of real-time data with low latency and high throughput. Flink uses a distributed dataflow programming model to process data in parallel across a cluster of machines. Flink supports both batch and stream processing, allowing users to analyze both historical and real-time data with the same programming model. Flink also provides fault tolerance and high availability mechanisms, ensuring that data processing is resilient in the face of failures.

Apache Flink is DataSQRL's reference implementation for a stream engine and supports all SQRL features.

### Database Engine
A database engine reliably persists data for concurrent query access.

DataSQRL currently supports the following database engines:

* Postgres: Postgres is an open-source relational database management system.
* Iceberg

### Server Engine
Currently, DataSQRL can compile GraphQL APIs with REST on the roadmap. The default server engine uses the Vertx framework for high performance workloads. Aws lambda server engines are on the roadmap.

### Log Engine
DataSQRL currently supports Kafka as it's default log engine. DataSQRL will create topics for mutations and subscription in the graphql api. It will also create a topic for `create table` commands.

### Query Engine
DataSQRL supports Snowflake as a query engine.

##### Script to GraphQL Schema Mapping
DataSQRL maps the tables, fields, and relationships defined in the SQRL script to a GraphQL schema which exposes the data through a GraphQL API.

##### GraphQL Schema Customization
The compiler generates *complete* GraphQL schemas, which means that the schema contains all tables, fields, and relationships defined in the SQRL script as well as field filters for all fields. In most cases, we don't want to expose all of those in the data API.

You can create your own custom GraphQL schema by trimming the generated schema and only expose those tables, fields, relationships, and filters that are required by your data API.

#### Adding Pagination
Pagination allows the user of an API to page through the results when there are too many results to return them all at once.
For our example, we might have thousands of orders and wouldn't want to return all of them when the user accesses the `Orders()` query end point of our API.

To limit the number of results the API returns and allow the user to page through the results to retrieve them incrementally, we add `limit` and `offset` arguments to query endpoints and relationship fields in GraphQL schema.
```graphql
type Query {
  Orders(time: String, limit: Int!, offset: Int = 0): [Orders!]
  Users(id: Int!): Users
}
```
The `Orders()` query endpoint requires a limit and an optional offset which defaults to `0`.

```graphql
type Users {
  id: Int!
  purchases(limit: Int!, offset: Int): [Orders!]
  spending(week: String, limit: Int = 20): [spending!]
}
```
In the type definition for `Users` above, we use `limit` and `offset` arguments to allow users of the API to page through the purchase history of a user and return a limited amount of spending analysis.

## Compiler and Tools
<!-- -->

### Compiler Workflow
<!-- -->
DataSQRL supports multiple engines and data pipeline architectures. That means, you can configure the architecture of the targeted data pipeline and what systems will execute individual components of the compiled data pipeline.

<img src="/img/reference/reactive_microservice.svg" alt="DataSQRL data pipeline architecture >" width="50%"/>

The figure shows a data pipeline architecture that consists of a Apache Kafka, Apache Flink, a database engine, and API server. Kafka holds the input and streaming data. Flink ingests the data, processes it, and writes the results to the database. The API server translates incoming requests into database queries and assembles the response from the returned query results.

The data pipeline architecture and engines are configured in the [package configuration](/docs/reference/sqrl/datasqrl-spec). The DataSQRL command looks for a `package.json` configuration file in the directory where it is executed. Alternatively, the package configuration file can be provided as an argument via the `-c` option. Check out the [command line reference](../cli) for all command line options.

If no package configuration file is provided or found, DataSQRL generates a default package configuration with the example data pipeline architecture shown above and the following engines:

* **Apache Flink** as the stream engine
* **Postgres** as the database engine
* **Vertx** as the API server
* **Kafka** as the API server

The package configuration contains additional compiler options and declares the dependencies of a script.

<!--
## Tools

### Packager

Explanation of preprocessors
Explanation of template engine
-->
## Debugging
DataSQRL supports a print sink to aid in debugging. The print data sink prints the data records in a stream to standard output.

```sql
EXPORT NewCustomerPromotion TO print.Promotion; 
```

This export statement prints all records in the `NewCustomerPromotion` stream table and uses the sink table name `Promotion` as the prefix in the output.

<!--
### API Security and Authorization

You can use any authentication layer with the compiled data API.

JWT-based authorization is on the roadmap.
-->
## Deployment and Operations

To deploy your DataSQRL project, the first step is to compile the deployment artifacts:

```bash
docker run --rm -v $PWD:/build datasqrl/cmd compile myscript.sqrl myapischema.graphqls
```

The compiler populates the `build/` directory with all the build artifacts needed to compile the data pipeline. Inside the build directory is the `deploy/` directory that contains all the deployment artifacts for the individual engines configured in the package configuration. If no package configuration is provided, DataSQRL uses the default engines.

You can either deploy DataSQRL projects with docker or deploy each engine separately. Using docker is the easiest deployment option.

Deploying each engine separately gives you more flexibility and allows you to deploy on existing infrastructure or use managed cloud services.
<!-- -->

The deployment artifacts can be found in the `build/deploy` folder. How to deploy them individually depends on the engines that you are using for your data pipeline.

### Docker {#docker}

To run the pipeline that DataSQRL compiles from your SQRL script and API specification, execute:

```bash
(cd build/deploy; docker compose up)
```

This command executes docker-compose with the template generated by the DataSQRL compiler in the `build/deploy` directory. It starts all the engines and deploys the produced deployments artifacts of the compiled data pipeline to the engines to run the entire data pipeline.

The API server which is exposed at `localhost:8888/`. <br />
You can now access the API and execute queries against it to test your script and the compiled data pipeline.

Use the keystroke `CTRL-C` to stop the running data pipeline. This will stop all engines gracefully.

To deploy a SQRL script and API specification with docker, run `docker-compose up` in the `build/deploy` folder:

```bash
(cd build/deploy; docker compose up)
```

:::info
To stop the pipeline, interrupt it with `CTRL-C` and shut it down with:
```bash
docker compose down -v
```
It's important to remove the containers and volumes with this command before launching another data pipeline to get updated containers.
:::
<!--
### Deploy on AWS

This documentation walks you through the steps of deploying a data pipeline compiled by DataSQLR on AWS managed services. Using managed services eliminates most of the operational burden of running a data pipeline, auto-scales each engine based on the amount of incoming data and API workload, and gets you up and running with a production-grade data pipeline in an under an hour.

To set up a DataSQRL data pipeline on AWS managed services, follow these 3 steps.

:::warn
This documentation is work in progress. Please check back soon.
:::


### Monitoring and Performance Tuning
Techniques for monitoring and optimizing performance.
### High Availability and Disaster Recovery
Planning for high availability and recovery from disasters.

## FAQs and Troubleshooting
### Common Issues and Solutions
Troubleshooting common problems and solutions.
### Performance Optimization Tips
-->
## Community and Support Resources

To get a preview of upcoming features and see what we are currently working on, take a look at the [roadmap](/docs/dev/roadmap/) which summarizes the big ticket items and epics that are scheduled for development. However, active discussion of the roadmap and feature requests happens in the [Slack community](/community). <br />
To report bugs and view a ticket-level breakdown of development, head to the [issue tracker](https://github.com/DataSQRL/sqrl/issues).

If you want to make a code contribution to DataSQRL or become a committer, please review [these suggestions](/community).
