# DataSQRL

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/DataSQRL/sqrl/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/DataSQRL/sqrl/tree/main)
[![Docs](https://img.shields.io/badge/docs-available-brightgreen.svg)](https://datasqrl.github.io/sqrl)
[![codecov](https://codecov.io/gh/datasqrl/sqrl/branch/main/graph/badge.svg)](https://codecov.io/gh/datasqrl/sqrl)
[![License](https://img.shields.io/github/license/datasqrl/sqrl.svg)](LICENSE)
[![Docker Image Version](https://img.shields.io/docker/v/datasqrl/cmd?sort=semver)](https://hub.docker.com/r/datasqrl/cmd/tags)
[![Maven Central](https://img.shields.io/maven-central/v/com.datasqrl/sqrl-root)](https://repo1.maven.org/maven2/com/datasqrl/sqrl-root/)

DataSQRL is a data framework for building MCP servers, RAG pipelines, data APIs, feature stores, training datasets, and data products with SQL. DataSQRL automates the construction of data pipelines from multiple sources of data with guaranteed consistency and reliability.

Define the data processing and interface in SQL and DataSQRL generates an integrated data pipeline that runs on your existing infrastructure with Docker, Kubernetes, or cloud-managed services.

![DataSQRL Pipeline Architecture](/documentation/static/img/diagrams/streaming_architecture.png)

## DataSQRL Features

* 🛡️ **Data Consistency Guarantees:** Exactly-once processing, data consistency across all outputs, schema alignment, data lineage tracking, and comprehensive testing framework.
* 🔒 **Production-grade Reliability:** Robust, highly available, scalable, observable data pipelines executed by trusted OSS technologies (Kafka, Flink, Postgres, Apache Iceberg).
* 🔗 **Realtime, Incremental, or Batch:** Flexibility to run realtime (millisecond), incremental (second to minutes), or batch updates without code changes. 
* 🚀 **Robust Operations:** Local development, CI/CD support, logging framework, reusable components, and composable architecture for reliable pipeline management.
* 🤖 **AI-native with Accuracy:**  Support for vector embeddings, LLM invocation, and ML model inference with accurate data delivery for LLM tooling interfaces.

To learn more about DataSQRL, check out [the documentation](https://docs.datasqrl.com/).

## Getting Started

This example builds a data pipeline that captures user token consumption via API, exposes consumption alerts via subscription, and aggregates the data for query access.

<!-- Add video tutorial -->

```sql title=usertokens.sqrl
/*+no_query */
CREATE TABLE UserTokens (
    userid BIGINT NOT NULL,
    tokens BIGINT NOT NULL,
    request_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp'
);

/*+query_by_all(userid) */
TotalUserTokens := SELECT userid, sum(tokens) as total_tokens,
                          count(tokens) as total_requests
                   FROM UserTokens GROUP BY userid;

UsageAlert := SUBSCRIBE SELECT * FROM UserTokens WHERE tokens > 100000;
```

Create a file `usertokens.sqrl` with the content above and run it with:

```bash
docker run -it --rm -p 8888:8888 -p 8081:8081 -p 9092:9092 -v $PWD:/build datasqrl/cmd:latest run usertokens.sqrl
``` 
(Use `${PWD}` in Powershell on Windows).

The pipeline is exposed through a GraphQL API that you can access at  [http://localhost:8888/graphiql/](http://localhost:8888/graphiql/) in your browser.

* `UserTokens` is exposed as a mutation for adding data.
* `TotalUserTokens` is exposed as a query for retrieving the aggregated data.
* `UsageAlert` is exposed as a subscription for real-time alerts.

Once you are done, terminate the pipeline with `CTRL-C`.

To build the deployment assets in the for the data pipeline, execute
```bash
docker run --rm -v $PWD:/build datasqrl/cmd:latest compile usertokens.sqrl
``` 
The `build/deploy` directory contains the Flink compiled plan, Kafka topic definitions, PostgreSQL schema and view definitions, server queries, and GraphQL data model.

Read the [full Getting Started tutorial](https://docs.datasqrl.com//docs/getting-started) or check out the [DataSQRL Examples repository](https://github.com/DataSQRL/datasqrl-examples/) for more examples creating Iceberg views, Chatbots, data APIs and more.

## Why DataSQRL?

Building data pipelines that power MCP servers, RAG, data APIs and data products requires implementing and maintaining lots tedious glue code. Making sure those data pipelines are robust, consistent, and governed requires diligent engineering work.

DataSQRL automates this error-prone busywork with a principled SQL-based abstraction layer so you can focus on what matters: your data.

## How DataSQRL Works

![Example Data Processing DAG](documentation/static/img/screenshots/dag_example.png)

DataSQRL compiles the SQL scripts and data source/sink definitions into a data processing DAG (Directed Acyclic Graph) according to the configuration. The cost-based optimizer cuts the DAG into segments executed by different engines (e.g. Flink, Kafka, Postgres, Vert.x), generating the necessary physical plans, schemas, and connectors for a fully integrated, reliable, and consistent data pipeline. These deployment assets are then executed in Docker, Kubernetes, or by a managed cloud service.

DataSQRL gives you full visibility and control over the generated data pipeline and uses proven open-source technologies to execute the generated deployment assets. DataSQRL is an extensible framework with support for custom functions, source/sink connectors, and data systems.

<!--
[DataSQRL Cloud](https://www.datasqrl.com) is a managed service that runs DataSQRL pipelines with no operational overhead and integrates directly with GitHub for simple deployments.
-->

Learn more about DataSQRL in [the documentation](https://docs.datasqrl.com/).


## Contributing

![Contribute to DataSQRL](documentation/static/img/undraw/code.svg)

Our goal is to simplify the development of data pipelines you can trust by automating the construction of robust and consistent data architectures. Your feedback is invaluable in achieving this goal. Let us know what works and what doesn't by filing GitHub issues or in the [DataSQRL Slack community]((https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg)).

We welcome code contributions. For more details, check out [`CONTRIBUTING.md`](CONTRIBUTING.md).

