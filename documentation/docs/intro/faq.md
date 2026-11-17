---
title: "Frequently Asked Questions"
description: "Common questions about DataSQRL: batch and streaming scope, existing infrastructure, lock-in, extending SQL, and enforcing organizational policies."
---

# Frequently Asked Questions

For how DataSQRL compares to dbt, SQLMesh, warehouse-native agents, Databricks, streaming databases, Flink, Spark, orchestrators, and plain coding agents, see [How DataSQRL Compares](comparison).

## Is DataSQRL only for real-time streaming?

No. Operational means data that moves between systems and is served to consumers, in real time or in batches. Batch ETL into Iceberg is in scope. Modeling inside a warehouse is not.

The same SQL runs as a streaming or a batch pipeline, depending on the [Flink runtime mode](../configuration-engine/flink). Batch pipelines can be split into [sequential sub-batches](../sqrl-language#next_batch). A common pattern is batch processing into Iceberg tables that are queried through DuckDB or Snowflake. You can also use DataSQRL to build data products and views in your existing data lake(house). See the [examples](examples) for both styles.

## Does it work with our existing infrastructure?

DataSQRL compiles to widely used open-source technologies: Apache Flink, Apache Kafka (and compatible systems such as Redpanda), PostgreSQL, and Apache Iceberg, with DuckDB, Spark, Trino, Snowflake, and others as query engines. It reads from and writes to your existing systems through [connectors](../connectors), and it deploys to Docker, Kubernetes, or managed cloud services. See [Configuration](../configuration) for the supported engines.

## Are we locked in? What if we stop using DataSQRL?

DataSQRL is open source under the Apache 2.0 license. Its output is standard deployment assets: Flink compiled plans, SQL schemas and queries, Kafka topic definitions, and GraphQL, REST, and MCP definitions. These run on standard open-source infrastructure with no proprietary runtime. You can also write and maintain SQRL by hand without an agent.

## What if SQL can't express my logic?

Extend it. DataSQRL supports [user-defined functions](../functions#user-defined-functions) in Java. Simple functions can be single-file JBang scripts, and complex ones can be full Java projects that define new table operators. You can also build your own function libraries that capture your business semantics, so agents and people reuse the same building blocks.

In addition, DataSQRL supports SQL passthrough to the underlying engines for SQL features that the underlying engine supports but DataSQRL does not yet, so that the abstraction layer does not become a limiting factor.

## How do we enforce our organization's standards and policies?

Customize the harness:

- **Skills and conventions** capture how your team gathers requirements, plans, tests, and deploys.
- **Custom validation rules** check governance, security, and data quality policies, such as PII handling or naming, on every compile.
- **Compile artifacts**, including the full data flow DAG, schemas, and lineage, feed audits and automated policy checks.

A [semantic data catalog](examples) with sensitivity and regulatory tags gives the agent the context to respect those policies from the start.
