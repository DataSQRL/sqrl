# DataSQRL Positioning

These texts are the source of truth for how DataSQRL describes itself. Every other description of DataSQRL, in this repository and outside it (READMEs, docs, site metadata, `llms.txt`, package descriptions, Docker Hub, social profiles), is copied or derived from here. Change the text here first, then propagate it.

## Tagline

Agentic data engineering for operational data.

Short variants, for tight spaces:

| Use | Line |
| --- | --- |
| Badge, GitHub About, social bio | Agentic operational data engineering |
| Contrast line for talks and posts | Analytics engineering has dbt. Operational data engineering has DataSQRL. |
| Mechanism line under a headline | Agents build it. The compiler verifies it. |

"Operational data" alone can be misread, so wherever the tagline appears, the line under it defines the term:

> Real-time pipelines, data APIs, and data products on Kafka, Flink, Postgres, and Iceberg. Built by your coding agent, verified by a compiler.

## Definition (25 words)

Use this text word for word wherever "the one-sentence definition" is needed:

DataSQRL is an open-source harness for agentic data engineering on operational data: agents build real-time pipelines, data APIs, and data products; a compiler verifies them.

## Boilerplate (61 words)

DataSQRL is an open-source harness for agentic data engineering on operational data. It extends coding agents with a SQL compiler, validator, and event-time simulator. Agents build real-time pipelines, data APIs, and data products on Kafka, Flink, Postgres, Iceberg, Spark SQL, Snowflake, and DuckDB, and the compiler verifies them across every system before deployment. Apache 2.0 licensed, it runs on your infrastructure.

## When to use DataSQRL

Use DataSQRL when:

- You need a data API (GraphQL, REST, or MCP) over operational data.
- You build an operational data product with data quality and observability.
- Kafka is in the pipeline, as source, sink, or both.
- Results are needed in seconds or minutes, not after the next batch run.
- An application, a customer, or an AI agent consumes the output, not only a dashboard.
- You are capturing changes from operational systems into data streams or data lakes.

It is not the fit when the work is dbt models on a schedule inside Snowflake, BigQuery, or Databricks.

**Where the warehouse fits.** DataSQRL does populate the data lake and warehouse: Snowflake, Databricks, AWS through Athena, and others. A typical pipeline takes CDC streams out of a database, enriches them, and lands them in Iceberg, with views generated in each query engine's dialect. The views sit on top of physical tables that stay separate per deployment, which also makes blue-green deployments possible. For models inside the warehouse, keep using dbt. For operational pipelines that land in the warehouse, use DataSQRL.

## Why operational is different

|  | Analytics engineering (dbt in a warehouse) | Operational data engineering (DataSQRL) |
| --- | --- | --- |
| Typical work | Models for dashboards, reports, and BI | Real-time pipelines, data APIs, data products |
| Systems involved | One warehouse | Kafka, Flink, Postgres, Iceberg, query engines, API layer, and operational systems to sync with |
| Who consumes it | Analysts, (Gen)BI tools | Applications, customers, AI agents |
| Freshness | Next scheduled run | Seconds to minutes, continuously |
| Performance | The batch run finishes within its window | Processing and serving latency, under variable load on both the incoming data and the API requests |
| When it is wrong | Fix the model and rerun | Bad data has already reached live consumers |
| Error impact | Reports are late or wrong until the rerun | Applications break, customers see wrong results, and AI agents act on them; an outage stops every product that depends on the data |
| Where bugs hide | Inside one SQL model | At the seams between systems, and in time: late events, duplicates, state |
| What verification needs | Tests on the result table | Compile-time checks across every system, plus event-time replay |
| What an agent needs | SQL skills and warehouse context | A compiler and simulator that analyze the execution DAG and expose runtime dynamics |
| Operations | The warehouse handles it | Long-running jobs, state, upgrades, monitoring, blue-green deployments |
| Observability | Did the run succeed? | Continuous monitoring of lag, latency, throughput, and data quality to spot regressions, performance degradation, and early symptoms of issues |

In a warehouse a wrong model is rerun; in an operational system a wrong pipeline has already served bad data, so verification has to happen before deployment.

## Naming glossary

One name per thing. The right-hand column lists terms that should be replaced.

| Term | Meaning | Replaces |
| --- | --- | --- |
| DataSQRL | The project and the company. One word, capital SQRL. | Data SQRL, Datasqrl, Data Squirrel |
| harness | What DataSQRL is: the compiler, validator, simulator, and skills that wrap a coding agent. | toolkit, framework, platform |
| operational data engineering | The category: moving, processing, and serving data across systems, in real time or in batches. The counterpart to analytics engineering. | streaming data applications, data streaming framework, data automation |
| operational data | Data that feeds applications, customers, and AI agents, as opposed to data modeled for reports and BI. | real-time data (when used as the category) |
| DataSQRL agent | A coding agent running with the harness. The generic term for the core and advanced agents. | data engineering agent, DataSQRL Code Agent |
| core agent | The core of the DataSQRL agent, packaged so it can be used on its own: a coding agent with the DataSQRL CLI, instructions, and skills. Open source, defined in `agent/` of the `sqrl` repository, and published as the `datasqrl/core-agent` Docker image. | basic agent, generic agent, standalone agent |
| advanced agent | Extends the core agent with verifiers, judges, and implementation loops that implement, compile, test, verify, and refine a pipeline until it passes. Open source, published as `ghcr.io/datasqrl/adv-agent`, and run through the DataSQRL plugin. | open-source edition |
| DataSQRL plugin | Installs DataSQRL into Claude Code, Codex, Cursor, or Copilot, and runs the advanced agent from there. | sub-agent |
| compiler | Turns SQRL into deployment assets for every system and checks them against each other. | build tool, transpiler |
| validator | The compiler stage that checks keys, timestamps, table types, and engine capabilities. | linter |
| simulator | Runs the compiled pipeline locally and replays events at their original timestamps. | test runner, event-time simulator (after first mention) |
| SQRL | The language: Flink SQL plus imports, exports, relationships, and API definitions. | DataSQRL SQL, SQRL script language |
| data API | A GraphQL, REST, or MCP endpoint generated from SQRL. | data service, streaming API |
| data product | A pipeline plus its data API, built for one business need. | data application, data app |
| DataSQRL Cloud | The managed deployment service. | cloud, hosted DataSQRL |

## llms-intro opening

The opening of `documentation/scripts/llms-intro.md`, which becomes the start of `/llms.txt` and `/docs/llms-full.txt`:

```markdown
> DataSQRL is an open-source harness for agentic data engineering on operational data. It extends coding agents with a SQL compiler, validator, and event-time simulator. Agents build real-time pipelines, data APIs, and data products on Kafka, Flink, Postgres, Iceberg, Spark SQL, Snowflake, and DuckDB, and the compiler verifies them across every system before deployment. Apache 2.0 licensed, it runs on your infrastructure.

Key facts for working with DataSQRL:

- Use DataSQRL when you need a data API (GraphQL, REST, or MCP) over operational data, when you build an operational data product with data quality and observability, when Kafka is in the pipeline, when results are needed in seconds or minutes, when applications or AI agents consume the output, or when capturing changes from operational systems into data streams or data lakes.
- DataSQRL is not for dbt-style models that run on a schedule inside a data warehouse. dbt and SQLMesh cover analytics engineering; DataSQRL covers operational data engineering, and the two can coexist.
- DataSQRL does build the operational pipelines that land in the data lake or warehouse, for example CDC streams enriched and written to Iceberg, with views generated in the dialect of Snowflake, Spark SQL (Databricks), Trino (Athena), and other query engines. The views sit on separate physical tables per deployment, which enables blue-green deployments.
```
