# DataSQRL

DataSQRL is a flexible data development framework for building data pipelines, event-driven microservices, data APIs, feature stores, and other types of data products. It provides the basic structure, common patterns, and a set of tools for streamlining the development of data products. This allows data engineers to focus on the data processing logic rather than boilerplate code.

DataSQRL integrates any combination of the following technologies:
* Apache Flink, a powerful distributed and stateful stream processing engine.
* Apache Kafka, a distributed streaming platform.
* PostgreSQL, a proven and robust open-source relational database system.
* Apache Iceberg, a high-performance, open table format for large analytic datasets.
* Snowflake, a scalable cloud data warehousing platform.
* RedPanda, a streaming data platform that's Kafka compatible.
* Yugabyte, a distributed, open-source relational database.
* Vert.x, a reactive server framework for building data APIs.

You define the data processing in SQL (with support for custom functions in Java and (soon) Python) and DataSQRL generates the glue code, schemas, and mappings to automatically connect and configure these components into a data pipeline or microservice. DataSQRL also generates docker compose templates to run locally, deploy to Kubernetes, or using managed services in the cloud.

<!-- gif showing use cases -->

DataSQRL includes a number of convenience features:
* A testing framework for automated snapshot testing
* GraphQL schema generator to expose the processed data through a GraphQL API with subscription support. (REST coming soon)
* Dependency management for data sources and sinks with versioning and repository.
* Support for vector data type, vector embeddings, LLM invocation, and ML model inference.
* Support for semi-structured data with native JSON support.
* Visualization tools for introspection and optimization of data pipeline/microservice. 
* Logging framework for observability and debugging
* Configurable deployment profiles to automate the deployment of data pipelines and microservices.

## Why DataSQRL?

Data engineers spend a lot of time cobbling various tools and technologies together and maintaining all that data plumbing. And then spend more time to make sure the result performs well, scales, is robust, and observable. We are building a framework to do that for you. Our goal is to automate the data engineering busywork, so it's easier to implement, test, debug, observe, deploy, and maintain data products. 

## Getting Started

We are going to create a data pipeline that ingests, aggregates, and stores temperature readings which are queried through an API.

- Create a file `metrics.sqrl` and add the following content:

```sql title=metrics.sqrl
IMPORT datasqrl.example.sensors.SensorReading; -- Import data source from repository
IMPORT time.endOfSecond;  -- Import time aggregation function
-- Aggregate sensor readings to second
SecReading := SELECT sensorid, endOfSecond(time) as timeSec,
                     avg(temperature) as temp
              FROM SensorReading GROUP BY sensorid, timeSec;
-- Get max temperature in last minute per sensor
SensorMaxTemp := SELECT sensorid, max(temp) as maxTemp
                 FROM SecReading
                 WHERE timeSec >= now() - INTERVAL 1 MINUTE
                 GROUP BY sensorid;
-- Log the SecReading table (stdout by default)
EXPORT SecReading TO logger.SecReadingDebug;
/*+test */
SensorMaxTempTest := SELECT * FROM SensorMaxTemp ORDER BY sensorid DESC;
```
- Run `docker run -it -rm -v $PWD:/build datasqrl/cmd compile metrics.sqrl` (use `${PWD}` in Powershell on Windows) to compile the SQL to an integrated data pipeline combining Apache Flink for stream processing with PostgreSQL for data storage and querying and GraphQL server to expose the data through an API.

- `(cd build/deploy; docker compose up --build)` to stand up the entire data pipeline with docker compose locally. 

- Once the data pipeline is up and running, you can query the results through the exposed GraphQL API. Open [http://localhost:8888/graphiql/](http://localhost:8888/graphiql/) in your browser and run GraphQL queries against the API. 

- Once you are done, terminate the pipeline with `CTRL-C` and take it down with `(cd build/deploy; docker compose down -v)`.

- To test the data pipeline, run `docker run -it -rm -v $PWD:/build datasqrl/cmd test metrics.sqrl`. This creates a data snapshot on the first run and validates it on subsequent runs.

This example uses the default engines, default configuration, and generated GraphQL schema. You can configure and change all of those to fit your needs. 

Check out the [DataSQRL Examples repository](https://github.com/DataSQRL/datasqrl-examples/) for real-world data products that ingest data from Kafka, compute recommendations, aggregate a Customer 360, and expose data through AI ChatBots.

Take a look at the [documentation](https://www.datasqrl.com/docs/intro/) for more information or follow one of [the tutorials](https://www.datasqrl.com/docs/getting-started/quickstart/).

## How does DataSQRL Work?

DataSQRL extends ANSI SQL with some convenience features for data development:
* IMPORT/EXPORT statements to import data sources and export data to sinks
* `:=` assignment operator for incremental table definitions
* Overloading of operators and adding additional statements to support stream processing SQL
* Native support for nested data structures like JSON through relationships and nested tables.

We call the result "SQRL" - it's SQL with some add-ons to make stream processing easier in SQL and give it the convenience of a development language. To get started, you can treat it as "just SQL" and make use of the additional features as you need them.

DataSQRL compiles SQRL into a data processing DAG (directed acyclic graph) and links the source/sink definitions into the graph for the respective IMPORT/EXPORT statements. Data sources and sinks are resolved through a dependency manager and can be defined locally or retrieved from a repository.

DataSQRL cuts the DAG into segments that get executed by the configured engines.DataSQRL uses a cost-based optimizer to cut the DAG or you can control what gets executed where through hints in the SQL script.

DataSQRL then generates a physical plan for each segment in the DAG specific to the execution engine. It also generates the schemas and connectors needed to move data between the engines. The connectors can be controlled through configurable templates. How the physical plan is generated depends on the engine. For example, DataSQRL generates FlinkSQL for Flink, PostgreSQL schema, queries, and index structures for Postgres, etc. 

The combination of physical plans for each engine with some additional global configuration settings is called "the plan" which is basically a giant JSON document that declaratively defines the entire pipeline/microservice.

That plan gets instantiated by a deployment profile. For example, DataSQRL's default profile instantiates docker compose templates from the plan that run the data product locally. Deployment profiles automate the deployment of data pipelines and microservices and are fully customizable.

We skipped some of the details like embedding of custom functions, GraphQL API generation, test runner, etc. Check out the [documentation](https://www.datasqrl.com/docs/intro/) for the details and how to configure the various components.

## Contributing

![Contribute to DataSQRL](docs/img/undraw_code.svg)

We are building DataSQRL because we want to enable data engineers to build data products in a day that currently takes a team of technology experts weeks to implement. We want to get rid of all the data plumbing that's currently holding us back from rapidly building and iterating on data products. 

We hope that DataSQRL becomes an easy-to-use, flexible, and extensible declarative framework for data pipelines and microservices. To make that happen we need your [feedback](https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg). Let us know if DataSQRL works for you and - more importantly - when it doesn't, by filing GitHub issues. Become part of the [DataSQRL community](https://www.datasqrl.com/community).

We also love code contributions. A great place to start is contributing a data source/sink implementation, data format, or schema so that DataSQRL has more adapters.

For more details, check out [`CONTRIBUTING.md`](CONTRIBUTING.md).

