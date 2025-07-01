---
slug: flinkrunner-announcement
title: "Flink SQL Runner: Run Flink SQL Without JARs or Glue Code"
authors: [matthias]
tags: [Flink, DataSQRL]
---

<head>
  <meta property="og:image" content="/img/blog/flinksqlrunner_logo.png" />
  <meta name="twitter:image" content="/img/blog/flinksqlrunner_logo.png" />
</head>

# Flink SQL Runner: Run Flink SQL Without JARs or Glue Code

Apache Flink has long been a powerhouse for streaming and batch data processing. And with the rise of Flink SQL, developers can now build sophisticated pipelines using a declarative language they already know. But getting Flink SQL applications into production still comes with friction: packaging JARs, managing connectors, injecting secrets, and wiring up deployment infrastructure.

<img src="/img/blog/flinksqlrunner_logo.png" alt="FlinkSQL Runner >" width="40%"/>

[**Flink SQL Runner**](https://github.com/DataSQRL/flink-sql-runner/) is here to change that. It's an open-source toolkit that simplifies development, deployment, and operation of Flink SQL applications—locally or in Kubernetes—without manual JAR assembly or scripting custom infrastructure pipelines.

<!--truncate-->

## From SQL to Production, Minus the Plumbing

Imagine you're writing a Flink SQL job that reads from Kafka, enriches the data, and sinks to Iceberg. In theory, it's just SQL. But in practice, production deployment requires:

* Assembling dependencies into a JAR
* Writing YAML to configure connectors
* Injecting secrets for different environments

Flink SQL Runner eliminates those headaches. You get:

* **Declarative execution** with SQL scripts or compiled plans
* **Simple deployments** on Kubernetes via Flink Operator
* **Environment isolation** with variable substitution and UDF packaging

All without leaving the SQL layer.

### Key Features

* **SQL and Plan Execution**: Run raw SQL scripts or pre-compiled execution plans.
* **Kubernetes-Native**: Built for the Flink Kubernetes Operator—deploy SQL jobs without writing infrastructure code.
* **Composable Toolkit**: Use the pieces you need—Docker image, libraries, extensions—to suit your environment.
* **Environment Variable Substitution**: Inject secrets and environment-specific config into SQL or plan files using `${ENV_VAR}` syntax.
* **UDF Infrastructure**: Load custom JARs and register system functions easily.
* **Function Libraries**: Drop-in UDFs for advanced math and OpenAI integration.


### Flexible and Extensible

Flink SQL Runner is not a monolith. You can:

* Run it standalone with Docker.
* Deploy it with the Flink Kubernetes Operator.
* Extend it via Maven or Gradle in your own Flink stack:

```xml
<dependency>
  <groupId>com.datasqrl.flinkrunner</groupId>
  <artifactId>flink-sql-runner</artifactId>
  <version>0.6.0</version>
</dependency>
```


## Get Started

The Flink SQL Runner project is open source and [available on Github](https://github.com/datasqrl/flink-sql-runner).
Check out the README for more information on how to use and deploy the Flink SQL Runner.

Try it out, report issues, or contribute your own UDFs.

