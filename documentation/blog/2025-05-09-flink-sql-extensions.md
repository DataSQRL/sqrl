---
slug: flinksql-extensions
title: "Defining Data Interfaces with FlinkSQL"
authors: [matthias]
tags: [Join, Flink, DataSQRL]
---

<head>
  <meta property="og:image" content="/img/blog/flinksql_extension_api.png" />
  <meta name="twitter:image" content="/img/blog/flinksql_extension_api.png" />
</head>

# Defining Data Interfaces with FlinkSQL

[FlinkSQL](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/) is an amazing innovation in data processing: it packages the power of realtime stream processing within the simplicity of SQL.
That means you can start with the SQL you know and introduce stream processing constructs as you need them.

<img src="/img/blog/flinksql_extension_api.png" alt="FlinkSQL API Extension >" width="40%"/>

FlinkSQL adds the ability to process data incrementally to the classic set-based semantics of SQL. In addition, FlinkSQL supports source and sink connectors making it easy to ingest data from and move data to other systems. That's a powerful combination which covers a lot of data processing use cases.

In fact, it only takes a few extensions to FlinkSQL to build entire data applications. Let's see how that works.

## Building Data APIs with FlinkSQL

```sql
CREATE TABLE UserTokens (
userid BIGINT NOT NULL,
tokens BIGINT NOT NULL,
request_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp'
);

/*+query_by_all(userid) */
TotalUserTokens := SELECT userid, sum(tokens) as total_tokens,
count(tokens) as total_requests
FROM UserTokens GROUP BY userid;

UserTokensByTime(userid BIGINT NOT NULL, fromTime TIMESTAMP NOT NULL, toTime TIMESTAMP NOT NULL):=
                SELECT * FROM UserTokens WHERE userid = :userid,
                request_time >= :fromTime AND request_time < :toTime ORDER BY request_time DESC;

UsageAlert := SUBSCRIBE SELECT * FROM UserTokens WHERE tokens > 100000;
```

This script defines a sequence of tables. We introduce `:=` as syntactic sugar for the verbose `CREATE TEMPORARY VIEW` syntax.

The `UserTokens` table does not have a configured connector, which mean we treat it as an API mutation endpoint connected to Flink via a Kafka topic that captures the events. This makes it easy to build APIs that capture user activity, transactions, or other types of events.

<!--truncate-->

Next, we sum up the data collected through the API for each user. This is a standard FlinkSQL aggregation query and we expose the result in our API through the `query_by_all` hint which defines the arguments for the query endpoint of that table.

We can also explicitly define query endpoints with arguments through SQL table functions. FlinkSQL supports table functions natively. All we had to do is provide the syntax for defining the function signature.

And last, the `SUBSCRIBE` keyword in front of the query defines a subscription endpoint for requests exceeding a certain token count which get pushed to clients in real-time.

Voila, we just build ourselves a complete GraphQL API with mutation, query, and subscription endpoints.
Run the above script with DataSQRL to see the result:

```bash
docker run -it --rm -p 8888:8888 -v $PWD:/build datasqrl/cmd run usertokens.sqrl
```

## Relationships for Complex Data Structures

And for extra credit, we can define relationships in FlinkSQL to represent the structure of our data explicitly and expose it in the API:

```sql
User.totalTokens := SELECT * FROM TotalUserTokens t WHERE this.userid = t.userid LIMIT 1;
```

The `User` table in this example is read from an upsert Kafka topic using a standard FlinkSQL `CREATE TABLE` statement.

## Code Modularity and Connector Management

Many FlinkSQL projects break the codebase into multiple files for better code readability, modularity, or to swap out sources and sinks. That requires extra infrastructure to manage FlinkSQL files and stitch them together.

How about we do that directly in FlinkSQL?

```sql
IMPORT source-data.User;
```

Here, we import the `User` table from a separate file within the `source-data` directory, allowing us to separate the data processing logic from the source configurations. It also enables us to use dependency management to swap out sources for local testing vs production.

And we can do the same for sinks:

```sql
EXPORT UsageAlert TO mysinks.UsageAlert;
```

In addition to breaking out the sink configuration from the main script, the `EXPORT` statement functions as an `INSERT INTO` statement and creates a `STATEMENT SET` implicitly. That makes the code easier to read.

## Learn More

[FlinkSQL](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/) is phenomenal extension of the SQL ecosystem to stream processing. With DataSQRL, we are trying to make it easier to build end-to-end data pipelines and complete data applications with FlinkSQL.

Check out the [complete example](/docs/intro/getting-started) which also covers testing, customization, and deployment. Or read the [documentation](/docs/sqrl-language) to learn more.