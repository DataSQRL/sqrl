# DataSQRL

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/DataSQRL/sqrl/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/DataSQRL/sqrl/tree/main)
[![Docs](https://img.shields.io/badge/docs-available-brightgreen.svg)](https://datasqrl.github.io/sqrl)
[![codecov](https://codecov.io/gh/datasqrl/sqrl/branch/main/graph/badge.svg)](https://codecov.io/gh/datasqrl/sqrl)
[![License](https://img.shields.io/github/license/datasqrl/sqrl.svg)](LICENSE)
[![Docker Image Version](https://img.shields.io/docker/v/datasqrl/cmd?sort=semver)](https://hub.docker.com/r/datasqrl/cmd/tags)
[![Maven Central](https://img.shields.io/maven-central/v/com.datasqrl/sqrl-root)](https://repo1.maven.org/maven2/com/datasqrl/sqrl-root/)

DataSQRL is a development framework for incremental and real-time data processing applications. Ingest data from various sources, integrate, transform, store, and serve the result as data APIs, LLM tooling, or iceberg views - with the simplicity of SQL.

Data Engineers use DataSQRL to quickly build production-ready data pipelines that:
* Create realtime **data APIs**,
* Serve accurate data as **tooling** for LLMs and agents,
* Populate **data lakehouses** with **Iceberg tables** and catalog views.

![DataSQRL Pipeline Architecture](/documentation/static/img/diagrams/streaming_architecture.png)

You define the data processing in SQL and DataSQRL compiles the entire data infrastructure with Apache Kafka, Flink, Postgres, Iceberg, GraphQL API, and LLM tooling. It generates the glue code, schemas, mappings, and deployment artifacts to automatically integrate and configure these components into a coherent data stack that is highly available, consistent, scalable, observable, and fast. DataSQRL supports quick local iteration, end-to-end pipeline testing, and deployment to Kubernetes or cloud-managed services.

## DataSQRL Features

* 🔗 **Eliminate Infrastructure Glue:** DataSQRL generates connectors, schemas, data mappings, SQL dialect translation, and configurations. Do more with less. 
* 🚀 **Develop faster:** Local development, CI/CD support, logging framework, reusable components, and composable architecture for quick iteration cycles. 
* 🛡️ **Accurate Data:** Consistent data processing with exactly or at-least once guarantees, testing framework, and data lineage.
* 🔒 **Production-grade:** Robust, highly available, scalable, observable, and executed by trusted OSS technologies (Kafka, Flink, Postgres, DuckDB).
* 🤖 **AI-native:**  Support for vector embeddings, LLM invocation, and ML model inference, and LLM tooling interfaces.

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

As data engineers, we got frustrated by all the data plumbing we had to implement, the lack of developer tooling, and the limited automation.

Web developers have frameworks to eliminate the busywork. We are building the DataSQRL framework to do the same for data engineers.

## How DataSQRL Works

![Example Data Processing DAG](documentation/static/img/screenshots/dag_example.png)

DataSQRL compiles the SQRL scripts and data source/sink definitions into a data processing DAG (Directed Acyclic Graph) according to the configuration. The cost-based optimizer cuts the DAG into segments executed by different engines (e.g. Flink, Kafka, Postgres, Vert.x), generating the necessary physical plans, schemas, and connectors for a fully integrated and streamlined data pipeline. These deployment assets are then executed in Docker, Kubernetes, or by a managed cloud service.

DataSQRL gives you full visibility and control over the generated data pipeline and uses proven open-source technologies to execute the generated deployment assets. 

<!--
[DataSQRL Cloud](https://www.datasqrl.com) is a managed service that runs DataSQRL pipelines with no operational overhead and integrates directly with GitHub for simple deployments.
-->

Learn more about DataSQRL in [the documentation](https://docs.datasqrl.com/).


## Contributing

![Contribute to DataSQRL](documentation/static/img/undraw/code.svg)

We aim to enable data engineers to build data pipelines quickly and eliminate the data plumbing busy work. Your feedback is invaluable in achieving this goal. Let us know what works and what doesn't by filing GitHub issues or in the [DataSQRL Slack community]((https://join.slack.com/t/datasqrlcommunity/shared_invite/zt-2l3rl1g6o-im6YXYCqU7t55CNaHqz_Kg)).

We welcome code contributions. For more details, check out [`CONTRIBUTING.md`](CONTRIBUTING.md).

