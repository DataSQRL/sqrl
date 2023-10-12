# DataSQRL

DataSQRL is a compiler for building data services and APIs from streaming, static, or external data sources. The DataSQRL compiler generates optimal data pipelines from the data transformations expressed in SQRL and the API specification of the data service. *"Structured Query and Reaction Language"* (SQRL) is a development language that simplifies and extends SQL with reactive and streaming concepts, relationships, and explicit data dependencies.

## Quickstart

- Create a file `seedshop.sqrl` and add the following content:
```sql
IMPORT datasqrl.seedshop.Orders;  -- Import orders stream
IMPORT time.startOfMonth;         -- Import time function
/* Augment orders with aggregate calculations */
Orders.items.total := quantity * unit_price - discount?0.0;
Orders.totals := SELECT sum(total) as price,
                      sum(discount) as saving FROM @.items;
/* Create new table of unique customers */
Users := SELECT DISTINCT customerid AS id FROM Orders;
/* Create relationship between customers and orders */
Users.purchases := JOIN Orders ON Orders.customerid = @.id;
/* Aggregate the purchase history for each user by month */
Users.spending := SELECT startOfMonth(p.time) AS month,
              sum(t.price) AS spend, sum(t.saving) AS saved
         FROM @.purchases p JOIN p.totals t
         GROUP BY month ORDER BY month DESC;
```
- Run `docker run -it -v $PWD:/build datasqrl/cmd compile seedshop.sqrl;(cd build/deploy; docker compose up)` 

This compiles the script into a data pipeline and executes the data pipeline against Apache Flink, Postgres, and a Vertx API server. You can inspect the resulting GraphQL API by navigating your browser to [http://localhost:8888/graphiql/](http://localhost:8888/graphiql/) and run GraphQL queries against the API. Hit `CTRL-C` to terminate the data pipeline when you are done. 

For an explanation of this example, take a look at the [Quickstart Tutorial](https://www.datasqrl.com/docs/getting-started/quickstart).

## Documentation

* [User Documentation](https://www.datasqrl.com/docs/intro) teaches you how to use DataSQRL to build data services.
* [Developer Documentation](https://www.datasqrl.com/docs/dev/overview) talks about the architecture and structure of the DataSQRL open-source project if you wish to contribute.
* [DataSQRL.com](https://www.datasqrl.com/) if you want to learn about DataSQRL in general.

> **Warning**
> This preview release of DataSQRL is intended for experimental use and feedback. Do not use DataSQRL in production yet.

## Key Contributions

Building data services is more productive with DataSQRL because the compiler generates all the plumbing, schema mapping, orchestration, and workflow management code that data service implementations require. In addition, it determines the optimal allocation of resources in the compiled data pipeline.

Specifically, DataSQRL makes the following contributions as a development environment for data services:

- A data dependency framework for external sources of data that allows developers to treat data dependencies like software dependencies.
- SQRL as a development language for data services that extends SQL by adding support for:
  - Reactive and stream processing
  - Relationships, nested, and semi-structured data
  - Incremental table definitions
- A compiler for SQRL scripts that produces complete data pipelines against configurable data infrastructure.
- A package manager that resolve data dependencies and supports pluggable configuration of data systems for execution.

The goal for DataSQRL is to become an abstraction layer over existing data technologies for building data services, APIs, and applications that makes developers more productive without sacrificing control or expressivity.

## Current Limitations

DataSQRL is currently a preview release that is not intended for production use. 

- DataSQRL has a pluggable infrastructure for "execution engines" (i.e. the data systems that comprise the data infrastructure DataSQRL compiles against) but currently it supports only [Apache Flink](https://flink.apache.org/) as a streaming engine, [PostgreSQL](https://www.postgresql.org/) as a database engine, and [Vert.x](https://vertx.io/) as a server engine.
- DataSQRL has a pluggable infrastructure for data sources and sinks (i.e. the locations it ingests data from and writes data to) but currently supports only local and remote filesystems and [Apache Kafka](https://kafka.apache.org/).
- The GraphQL schema parser is currently limited in the kinds of transformations that it allows to the GraphQL schema file. Currently, field names need to map exactly onto those defined in the script.
- The DataSQRL planner has some inefficiencies in handling nested data, limited self-join elimination, and limited temporal join support.
- The DataSQRL optimizer currently uses a trivial cost model and does not yet produce optimal results.
- DataSQRL has limited error handling, observability, and monitoring support.

## Contributing

We built DataSQRL because we got tired of the endless plumbing, data mapping, and orchestration that is currently needed when building data services. We hope that DataSQRL becomes an easy-to-use abstraction layer that eliminates all the tedium and allows you to focus on the purpose and function of your data services. To make that happen we need your [feedback](https://discord.gg/vYyREMNRmh). Let us know if DataSQRL works for you and - more importantly - when it doesn't. Become part of the [DataSQRL community](https://www.datasqrl.com/community).

We also love [code contributions](https://www.datasqrl.com/docs/dev/contribute). A great place to start is contributing a data source/sink implementation, data format, or schema so that DataSQRL can cover more use cases.

For more details, checkout [`CONTRIBUTING.md`](CONTRIBUTING.md)

