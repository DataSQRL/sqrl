# DataSQRL

DataSQRL is a development environment for building data services from streaming, static, or external data sources. The DataSQRL compiler generates optimal data pipelines from the data transformations expressed in SQRL and the API specification of the data service. *"Structured Query and Reaction Language"* (SQRL) is a development language that simplifies and extends SQL with reactive and streaming concepts, relationships, and explicit data dependencies.

> **Warning**
> This preview release of DataSQRL is intended for experimental use and feedback. Do not use DataSQRL in production yet.

## Quickstart

### Step 1: Build and Install DataSQRL

- Clone the DataSQRL repository (hit the green *Code* button at the top)
- Run `mvn clean package -DskipTests` in the `sqrl` folder of the cloned repository on your hard drive (you need mvn 3.6+ and Java JDK 11+). We will refer to this folder as `[SQRL-PATH]` in the following.
- Execute `sqrl-run/datasqrl.sh -h` which prints out the help for the `datasqrl` command. 

Note: Running the DataSQRL compiler requires Java 11. You may have to set your `JAVA_HOME` in order to use Java 11 explicitly either in front of the command or in your environment. For example, on MAC you would run:
```
JAVA_HOME=`/usr/libexec/java_home -v11` sqrl-run/datasqrl.sh -h
```

### Step 2: Build a Data Artifact

DataSQRL treats external data sources and sinks like software dependencies that can be imported and managed. To import a data source, we first create a data artifact:

- Create a directory where you want to create your first DataSQRL project: `mkdir myproject; cd myproject`
- Create a data artifact for the example Nutshop data: `[SQRL-PATH]/sqrl-run/datasqrl.sh discover [SQRL-PATH]/sqrl-examples/nutshop/data-small/ -o nutshop-small`

The `discover` command analyzes a data source and produces a data artifact in the specified output folder (i.e. `nutshop-data`). Try this with your own data.

### Step 3: Create and Run a SQRL Script

- Create a file `myscript.sqrl` and add the following content:
```sql
/* Imports the Products and Orders table from the Nutshop data
   We set an explicit timestamp on the Orders stream table */
IMPORT nutshop-small.Products;
IMPORT nutshop-small.Orders TIMESTAMP epoch_to_timestamp(time/1000) AS timestamp;

/* Some order items are missing the discount field - let's clean that up */
Orders.items.discount := coalesce(discount, 0.0);

/* The Customers table are all the `customerid` that have placed an order */
Customers := SELECT DISTINCT customerid AS id FROM Orders;
/* All imported tables are streams - we convert that stream into a state table by
   picking the most recent version for each product by `id` */
Products := DISTINCT Products ON id ORDER BY updated DESC;

/* Defines a relationship `purchases` between Customers and Orders */
Customers.purchases := JOIN Orders ON Orders.customerid = @.id ORDER BY Orders.time DESC;
/* and a relationship from Orders to Products */
Orders.items.product := JOIN Products ON Products.id = @.productid;
```
- Run `[SQRL-PATH]/sqrl-run/datasqrl.sh run myscript.sqrl` 

This compiles the script into a data pipeline and executes the data pipeline against Apache Flink, Postgres, and a Vertx API server. You can inspect the resulting GraphQL API by navigating your browser to [http://localhost:8888/graphiql/](http://localhost:8888/graphiql/) and run GraphQL queries against the API. Hit `CTRL-C` to terminate the data pipeline when you are done. 

For a full example of SQRL scripts for our Nutshop, check out the [annotated SQRL example](sqrl-examples/nutshop/customer360/nutshopv1-small.sqrl) or the [extended SQRL example](sqrl-examples/nutshop/customer360/nutshopv2-small.sqrl)

### Step 4: Customize GraphQL API

Run the `compile` command with the `-s` flag and DataSQRL will write the generated GraphQL schema for the resulting API in the file `schema.graphqls`. We can modify the GraphQL schema to adjust the API to our needs.

DataSQRL generates a very flexible API. Let's trim that down to only the access points that we need. Change the file `schema.graphqls` to contain the following:
```graphql
type Customers {
  id: Int
  purchases(time: Int): [orders]
}

type Products {
  id: Int
  name: String
  sizing: String
  weight_in_gram: Int
  type: String
  category: String
}

type orders {
  id: Int
  customerid: Int
  time: Int
  items: [items]
  timestamp: String
}

type items {
  productid: Int
  quantity: Int
  unit_price: Float
  discount: Float
  product: Products
}

type Query {
  Customers(id: Int): [Customers]
  orders(id: Int!): [orders]
  Products: [Products]
}
```

- Save the `schema.graphqls` file.
- Run `[SQRL-PATH]/sqrl-run/datasqrl.sh run myscript.sqrl schema.graphqls`

This time, the compiler generates an updated data pipeline to produce our custom API. You can inspect the results by navigating your browser to [http://localhost:8888/graphiql/](http://localhost:8888/graphiql/).

## Key Contributions

Building data services is more productive with DataSQRL because the development environment generates all the plumbing, schema mapping, orchestration, and workflow management code that data service implementations requires. In addition, it determines the optimal allocation of resources in data pipelines.

Specifically, DataSQRL makes the following contributions as a development environment for data services:

- A data dependency framework for external sources of data that allows developers to treat data dependencies like software dependencies.
- SQRL as a development language for data services that extends SQL by adding support for:
  - Reactive and stream processing
  - Relationships, nested, and semi-structured data
  - Incremental table definitions
- A compiler for SQRL scripts that produces complete data pipelines against configurable data infrastructure.
- A package manager that resolve data dependencies and supports pluggable configuration of data systems for execution.

DataSQRL is analogous to an operating system for data. Much like an operating system orchestrates multiple pluggable pieces of hardware (e.g. CPU, RAM, hard drive) behind a uniform interface, DataSQRL orchestrates multiple pluggable data systems (e.g. database, stream engine, API server) behind a unified development environment.

## Current Limitations

DataSQRL is currently a working prototype that is not intended for production use. 

- DataSQRL has a pluggable infrastructure for "execution engines" (i.e. the data systems that comprise the data infrastructure DataSQRL compiles against) but currently it supports only [Apache Flink](https://flink.apache.org/) as a streaming engine, [PostgreSQL](https://www.postgresql.org/) as a database engine, and [Vert.x](https://vertx.io/) as a server engine.
- DataSQRL has a pluggable infrastructure for data sources and sinks (i.e. the locations it ingests data from and writes data to) but currently supports only local and remote filesystems and [Apache Kafka](https://kafka.apache.org/).
- DataSQRL has a pluggable infrastructure for data formats but currently only supports json and csv formats.
- The GraphQL schema parser is currently limited in the kinds of transformations that it allows to the GraphQL schema file. Currently, field names need to map exactly onto those defined in the script.
- The DataSQRL planner has some inefficiencies in handling nested data, limited self-join elimination, and limited temporal join support.
- The DataSQRL optimizer currently uses a trivial cost model and does not yet produce optimal results.
- DataSQRL has limited error handling, observability, and monitoring support.

## Contributing

We built DataSQRL because we got tired of the endless plumbing, data mapping, and orchestration that is currently needed when building data services. We hope that DataSQRL becomes an easy-to-use abstraction layer that eliminates all the tedium and allows you to focus on the purpose and function of your data services. To make that happen we need your feedback. Let us know if DataSQRL works for you and - more importantly - when it doesn't.

We also love code contributions. A great place to start is contributing a data source/sink implementation, data format, or execution engine so that DataSQRL can cover more environments and use cases.

For more details, checkout [`CONTRIBUTING.md`](CONTRIBUTING.md) as well as the [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)

