# DataSQRL

DataSQRL compiles SQL to optimized data pipelines and data microservices, eliminating the manual work of integrating and tuning data architectures that have multiple steps or components.

DataSQRL compiles SQL plus an (optional) API definition into an realtime data pipeline that processes data according to the SQL transformations, serves the results through a database, and (optionally) exposes them through a responsive API.

You declaratively define your data sources (in JSON), your data processing (in SQL), and optionally your data serving API (in GraphQL) which DataSQRL compiles to an integrated data pipeline based on Apache Flink, database, and optionally API server.

[![DataSQRL Compiled Data Pipeline](docs/img/pipeline_example.svg)](https://youtu.be/kq6SDR-TJpM)

DataSQRL has a pluggable engine model that supports technologies like Apache Kafka, Apache Flink, Postgres, and Eclipse Vert.x to execute the steps of the pipeline. The topology of the data pipeline is defined in a JSON package file which specifies the engines to use in the pipeline. DataSQRL's optimizer builds efficient data pipelines against the configured engines that optimize partial view materialization, physical data models, partitioning, and data flow. <br /> 
DataSQRL has adapters for various data sources like Apache Kafka, file system, S3, etc. Additional engines and adapters can be added to the compiler (it's open-source, wink wink).

What [this video](https://youtu.be/kq6SDR-TJpM) for an explanation of what DataSQRL does, how it works, and how to use it.

## Why DataSQRL?

DataSQRL makes it easier to build efficient data pipelines that:

* have multiple processing steps, because DataSQRL can optimize the resulting computational DAG automatically.
* combine stream processing with database querying, because DataSQRL manages the integration between stream and database and provides an abstraction layer for a streaming database.
* expose processed data through an API, because DataSQRL generates an API server and integration into the data pipeline from an API specification.
* require iterative development, because DataSQRL provides developer tooling that automates data dependency management and provides instant feedback.

DataSQRL provides these benefits through a purposeful abstraction layer, compile-time optimization, and developer tooling. That means, DataSQRL might *not* be a good choice for use cases that require extremely high performance (i.e. millions of records a second or more) where every implementation detail needs to be optimized.

## Example

We are going to create a data pipeline that ingests temperature readings and aggregates them for realtime querying.

- Create a file `metrics.sqrl` and add the following content:

```sql title=metrics.sqrl
IMPORT datasqrl.example.sensors.SensorReading; -- Import metrics
IMPORT time.endOfSecond;  -- Import time function
/* Aggregate sensor readings to second */
SecReading := SELECT sensorid, endOfSecond(time) as timeSec,
                     avg(temperature) as temp
              FROM SensorReading GROUP BY sensorid, timeSec;
/* Get max temperature in last minute per sensor */
SensorMaxTemp := SELECT sensorid, max(temp) as maxTemp
                 FROM SecReading
                 WHERE timeSec >= now() - INTERVAL 1 MINUTE
                 GROUP BY sensorid;
```
- Run `docker run -it -p 8888:8888 -p 8081:8081 -v $PWD:/build datasqrl/cmd run metrics.sqrl` to compile the SQL script and run the resulting data pipeline (use `${PWD}` in Powershell on Windows). This requires that you have Docker installed and takes a minute to start up.

Once the data pipeline is up and running, you can query the results through the default GraphQL API that DataSQRL generates. Open [http://localhost:8888/graphiql/](http://localhost:8888/graphiql/) in your browser and run GraphQL queries against the API. Once you are done, terminate the pipeline with `CTRL-C`.

For a detailed explanation of this example, take a look at the [Quickstart Tutorial](https://www.datasqrl.com/docs/getting-started/quickstart) which shows you how to customize the API, create realtime alerts, and more.

## DataSQRL Features

Building data pipelines manually is tedious because of all the integration code (e.g. connectors, schema mappings, error handling, retry logic, ...) and low-level implementation details (e.g. timestamp synchronization, physical data modeling, denormalization, index selection, view maintenance, ...) one has to implement.

The idea behind DataSQRL is to "compile away" most of this data plumbing code by:
1. Using SQL to implement the data flow of a data pipeline and using a cost-based optimizer to map data processing to best suited step in the pipeline.
2. Using versioned dependency management for data sources and sinks to automate connector handling through a package manager.
3. Compile-time optimization of low-level implementation details like watermarks, physical data models, and index structures so they can be abstracted away.
4. Typing of relational tables, operator overloading, and schema validation to provide compile-time feedback and simplify development.

In short, we are trying to apply the lessons we learned in software engineering over the last three decades to data engineering by using tooling to automate much of the manual coding required for integrating data pipelines.

The goal for DataSQRL is to become a declarative framework for implementing pipelines of various topologies that abstracts away the data plumbing code required to stitch data pipelines together without limiting expressivity or flexibility.

## Documentation

* [Getting Started](https://www.datasqrl.com/docs/getting-started/overview/) gets you up and running with DataSQRL. 
* [User Documentation](https://www.datasqrl.com/docs/intro) the complete documentation for DataSQRL.
* [DataSQRL.com](https://www.datasqrl.com/) if you want to learn about DataSQRL in general.

## Current Limitations

DataSQRL is still very young and limited in the types of technologies and data sources/sinks it can support. Here are some of the rough edges to be aware of:

- DataSQRL has a pluggable infrastructure for "execution engines", i.e. the data systems that run the executables DataSQRL compiles for each step in the data pipeline. Currently, DataSQRL supports only [Apache Kafka](https://flink.apache.org/) as a data stream, [Apache Flink](https://flink.apache.org/) as a stream processor, [PostgreSQL](https://www.postgresql.org/) as a database, and [Vert.x](https://vertx.io/) as a API server. Additional execution engines will be added over time.
- DataSQRL has adapters for data sources and sinks (i.e. the locations it ingests data from and writes data to) but currently supports only local and remote filesystems (like S3) and Apache Kafka.
- DataSQRL supports custom functions and data types. However, only JVM based languages are supported currently for implementing such extensions.
- The DataSQRL optimizer currently uses a simple cost model and does not yet produce optimal results in all cases. DataSQRL supports SQL hints to manually overwrite the optimizer.
- DataSQRL does not yet support the entire SQL standard, specifically certain types of `WINDOW` queries and complex subqueries. We'll get there.

## Contributing

![Contribute to DataSQRL](docs/img/undraw_code.svg)

We are building DataSQRL because we want to enable data engineers to build data pipelines in a day that currently takes a team of technology experts weeks to implement. We want to get rid of all the data plumbing that's currently holding us back from rapidly building and iterating on data products. 

We hope that DataSQRL becomes an easy-to-use declarative framework for data pipelines. To make that happen we need your [feedback](https://discord.gg/49AnhVY2w9). Let us know if DataSQRL works for you and - more importantly - when it doesn't by filing GitHub issues. Become part of the [DataSQRL community](https://www.datasqrl.com/community).

We also love [code contributions](https://www.datasqrl.com/docs/dev/contribute). A great place to start is contributing a data source/sink implementation, data format, or schema so that DataSQRL has more adapters.

For more details, check out [`CONTRIBUTING.md`](CONTRIBUTING.md).

