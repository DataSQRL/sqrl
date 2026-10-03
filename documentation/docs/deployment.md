# Deployment

The [compiler](compiler) generates all deployment assets for a pipeline in `build/deploy/plan`: Flink plans, Kafka topic definitions, database schemas, and the API server model (see [Compiler Output](compiler-output)). This page gives an overview of how to deploy those assets to production.

:::tip DataSQRL Cloud
The easiest way to deploy is [DataSQRL Cloud](https://www.datasqrl.com). It deploys your pipeline directly from your GitHub repository and supports the deployment options in your `package.json` configuration, such as instance sizes and replica counts (see [Cloud Deployment Configuration](configuration-engine/cloud-deployment)). DataSQRL Cloud is currently in closed beta. [Apply for access](https://www.datasqrl.com) to try it.
:::

## What Gets Deployed

Every deployment consists of the same components, regardless of where they run:

| Component     | Deployment assets                                                     | Runs with                                                                                  |
|---------------|-----------------------------------------------------------------------|--------------------------------------------------------------------------------------------|
| Flink job     | `flink-compiled-plan.json`, `flink-config.yaml`, `flink-functions.sql` | The [Flink SQL Runner](https://github.com/DataSQRL/flink-sql-runner), which executes the compiled plan and provides the DataSQRL function library and connectors |
| Kafka topics  | `kafka.json`                                                          | Any Kafka-compatible service                                                               |
| Database      | `postgres-schema.sql`, `postgres-views.sql`                           | PostgreSQL, with the extensions listed in `postgres.json`                                  |
| Data lake     | `iceberg-schema.sql` and the query engine schema                      | Object storage with an Iceberg catalog, plus the configured [query engine](configuration-engine/iceberg-query) |
| API server    | `vertx.json`, `vertx-config.json`                                     | The [`datasqrl/sqrl-server`](https://hub.docker.com/r/datasqrl/sqrl-server) Docker image, which serves GraphQL, REST, and MCP |

A deployment creates the Kafka topics and database schemas first, then starts the API server and the Flink job. Only the components for the engines enabled in your [configuration](configuration) are generated.

## Option A: Managed Cloud Services

Managed services are the easiest way to operate a pipeline yourself, because the cloud provider handles availability, upgrades, and backups. Each component maps to services you may already use:

- **Flink:** Amazon Managed Service for Apache Flink, or a managed Flink platform such as Confluent, runs the Flink SQL Runner as an application JAR with the compiled plan or the compiled plan directly. Check that the service supports the Flink version DataSQRL uses (see [Compatibility](compatibility)).
- **Kafka:** Amazon MSK, Azure Event Hubs (through its Kafka endpoint), Confluent Cloud, or Redpanda Cloud.
- **PostgreSQL:** Amazon RDS or Aurora, Azure Database for PostgreSQL, or Google Cloud SQL.
- **Iceberg:** Amazon S3, Azure Data Lake Storage, or Google Cloud Storage with an Iceberg catalog, queried by a managed engine such as Spark SQL, Snowflake, Redshift, Athena, or Trino.
- **API server:** any container service, such as Amazon ECS, Azure Container Apps, or Google Cloud Run, runs the server image with the server model and configuration mounted.

## Option B: Kubernetes

Kubernetes gives you the most control over the deployment, and it runs the same way on any cloud or on premises. Use operators to run each component:

- **Flink:** the [Apache Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-stable/) manages the Flink SQL Runner as a `FlinkDeployment`, including checkpoints, savepoints, and upgrades.
- **Kafka:** [Strimzi](https://strimzi.io/) or the Redpanda operator.
- **PostgreSQL:** [CloudNativePG](https://cloudnative-pg.io/) runs highly available PostgreSQL clusters with replication and backups.
- **Iceberg:** object storage and an Iceberg REST catalog, with the query engine running in the cluster or as a managed service.
- **API server:** a standard `Deployment` and `Service` for the server image, scaled horizontally since the server is stateless. It exposes `/health` for probes and, when enabled, `/metrics` for Prometheus.

You can also mix both options, for example running Flink and the API server on Kubernetes against managed Kafka and PostgreSQL services.

## Working Out the Details

The details of a production deployment depend on your infrastructure, security requirements, and existing tooling, such as Terraform, Helm, or your CI/CD pipeline. Consult the documentation of the services and operators you choose, or ask your coding agent to work out the deployment: point it at the `build/deploy/plan` folder, the [Flink SQL Runner](https://github.com/DataSQRL/flink-sql-runner) repository, and your target infrastructure, and have it generate the deployment configuration.
