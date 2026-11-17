---
slug: operational-data-engineering
title: "Operational Data Engineering Is Not Analytics Engineering"
description: "Analytics engineering models data inside a warehouse. Operational data engineering moves and serves data across systems to live consumers. Why the difference decides what a data engineering agent needs."
authors: [matthias]
tags: [agentic, data-engineering, DataSQRL]
---

# Operational Data Engineering Is Not Analytics Engineering

Analytics engineering has dbt. Operational data engineering has no name of its own, and that gap shows up every time someone asks how a coding agent should build data pipelines. The answer depends on which of the two worlds the pipeline lives in.

**Analytics engineering** models data inside a warehouse for dashboards and reports. **Operational data engineering** moves and serves data across systems, in real time or in batches, to applications, customers, and AI agents. Both use SQL. Both benefit from coding agents. But they fail in different ways, so they need different guardrails.

<!-- truncate -->

## Two worlds, side by side

|  | Analytics engineering (dbt in a warehouse) | Operational data engineering |
| --- | --- | --- |
| Typical work | Models for dashboards and reports | Real-time pipelines, data APIs, data products |
| Systems involved | One warehouse | Kafka, Flink, Postgres, Iceberg, API layer, and operational systems to sync with |
| Who consumes it | Analysts, (Gen)BI tools | Applications, customers, AI agents |
| Freshness | Next scheduled run | Seconds to minutes, continuously |
| When it is wrong | Fix the model and rerun | Bad data has already reached live consumers |
| Where bugs hide | Inside one SQL model | At the seams between systems, and in time: late events, duplicates, state |
| What verification needs | Tests on the result table | Compile-time checks across every system, plus event-time replay |
| What an agent needs | SQL skills and warehouse context | A compiler and simulator that catch what the agent cannot see |
| Operations | The warehouse handles it | Long-running jobs, state, upgrades, monitoring |

One row carries the argument: in a warehouse a wrong model is rerun; in an operational system a wrong pipeline has already served bad data, so verification has to happen before deployment.

## One system versus many

A dbt project runs inside one warehouse. The warehouse owns the types, the storage, the execution, and the operations. When an agent writes a model, the warehouse checks most of what can go wrong, and the rest shows up in a test on the result table.

An operational pipeline spans several systems. A Kafka topic carries the events, Flink processes them, Postgres or Iceberg stores the results, and an API serves them. Each system has its own schema, its own types, its own notion of a key, and its own configuration. Nothing checks that they agree. A coding agent can write correct Flink SQL and still get a key wrong between Flink and Postgres, or map a timestamp differently in the topic and the table. Those bugs live [at the seams between systems](/blog/p1-broken-at-seams), and no single system's tests see them.

## Time is part of the logic

A warehouse model reads a table that has stopped changing. An operational pipeline reads data that arrives continuously and out of order. Late events, duplicates, races between streams, idle sources, updates, and deletes are part of normal operation. A join that is correct on a static snapshot can be wrong when one side arrives a minute late.

These bugs do not show up in the query text, and they do not show up in a test against fixed fixtures. They show up when events [are replayed at their original timestamps](/blog/p3-testing-framework), which is a different kind of test than a check on a result table.

## The consumer is live

The consumer of a warehouse model is usually a person looking at a dashboard. If the number is wrong, someone notices, the model is fixed, and the next run corrects it.

The consumer of an operational pipeline is an application, a customer, or an AI agent, and it acts on the data as soon as it arrives. A wrong result has already been served, acted on, and possibly passed along. Rerunning the pipeline does not undo that. For operational data, quality has to be proven before deployment.

## What this means for agents

For analytics engineering, a coding agent mostly needs SQL skills and context about the warehouse: which tables exist, what the columns mean, which conventions the team follows. The warehouse-native agents being built today do exactly that, and they do it well.

For operational data engineering, context is not enough. The agent needs feedback from tools that see what it cannot: a compiler that generates the seams between systems from one model so they cannot disagree, a validator that checks keys, timestamps, and table types across the whole pipeline, and a simulator that replays event time. That is what [DataSQRL](/docs/intro) provides: an open-source harness for agentic data engineering on operational data. Agents build real-time pipelines, data APIs, and data products; a compiler verifies them.

## Use both

The two worlds meet in the data lake and warehouse. Operational pipelines often land their results there, for example CDC streams enriched and written to Iceberg, with views for the warehouse's query engine. The rule is simple: models inside the warehouse stay in dbt; operational pipelines that land in the warehouse are DataSQRL's. Many teams run both.

If you are not sure which one your problem is, [How DataSQRL Compares](/docs/intro/comparison) lists the fit triggers: a data API over operational data, Kafka in the pipeline, results needed in seconds or minutes, or applications and AI agents as consumers. If any of those apply, you are doing operational data engineering.
