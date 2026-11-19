---
slug: operational-data-engineering
title: "Operational Data Engineering Is Not Analytics Engineering"
description: "Analytics engineering models data inside a warehouse. Operational data engineering moves, processes, and serves data across systems to live consumers. Why the difference decides what a data engineering agent needs."
authors: [matthias]
tags: [agentic, data-engineering, DataSQRL]
---

# Operational Data Engineering Is Not Analytics Engineering

Ask ten people what a data engineer does and you get ten answers. One builds Spark jobs. One maintains Kafka clusters. One writes ETL into the warehouse. One keeps the CDC pipelines alive so the search index stays fresh. They all have the same job title.

"Data engineering" grew up as a catch-all for anything that moves data from where it is to where it's needed. As data engineering matured, sub-areas started to emerge.

dbt drew the first clear boundary. It took the work of turning raw warehouse tables into models for dashboards and reports, and gave it a name: **analytics engineering**. The boundary is the warehouse. Inside it, the work is SQL models, version control, tests, and documentation. A clear boundary led to a clear process, and a clear process led to tooling everyone now takes for granted.

Another clearly defined sub-area is **operational data engineering**: It moves, processes, and serves data across systems, in real time or in batches, to consumers that act on the data the moment it arrives or becomes available.

Both use SQL. Both benefit from coding agents. But they fail in different ways, and that decides what an agent needs to do the job well.

<!-- truncate -->

## It starts with the consumer

The clearest way to tell the two apart is to ask who consumes the data.

In analytics engineering, the consumer is a person inside your organization. An analyst looks at a dashboard, a BI tool runs a report, an executive reads a number in a meeting. The data needs to be fresh as of the last scheduled run. Performance means the batch finishes within its window. When a model is wrong, someone notices, the model gets fixed, and the next run corrects it. The report was late or wrong for a while. That's annoying, but it's recoverable.

In operational data engineering, the consumer is a live system or customer. An application calls an API to decide what a customer sees. A workflow reads the latest state to trigger the next step. An AI agent queries the data and acts on the answer. These consumers need fresh data and they need it instantly on request or pushed to them. Performance means processing and serving latency under variable load, both on the incoming events and on the API requests.

And when the data is wrong, it has already been served. The application broke, the customer saw the wrong price, the agent took the wrong action. Rerunning the pipeline doesn't undo any of that. An outage doesn't delay a report. It stops every product that depends on the data.

That's why operational data comes with SLAs and analytics data mostly doesn't. Live consumers need guarantees on freshness, latency, availability, and correctness. Those guarantees have to be proven before deployment, not discovered after.

## One system versus many

A dbt project runs inside one warehouse. The warehouse owns the types, the storage, the execution, and the operations. When an agent writes a model, the warehouse checks most of what can go wrong. A test on the result table catches the rest.

Live consumers rarely sit next to a warehouse. Serving them means integrating several systems. A Kafka topic carries the events, Flink processes them, Postgres or Iceberg stores the results, and an API layer serves them. Often the results also need to sync back into operational systems.

Each of those systems has its own schema, its own types, its own notion of a key, and its own configuration. Nothing checks that they agree. A coding agent can write correct Flink SQL and still get a key wrong between Flink and Postgres, or map a timestamp differently in the topic and the table. Those bugs live [at the seams between systems](/blog/p1-broken-at-seams), and no single system's tests see them.

The operations look different too. In a warehouse, "did the run succeed?" is most of the monitoring. An operational pipeline is a set of long-running jobs with state that has to survive upgrades and blue-green deployments. You watch lag, latency, throughput, and data quality continuously, because regressions show up as a slow drift long before they show up as an outage.

## Time is part of the logic

A warehouse model reads a table that has stopped changing. An operational pipeline reads data that arrives continuously and out of order. Late events, duplicates, races between streams, idle sources, updates, and deletes are normal operation. A join that is correct on a snapshot can be wrong when one side arrives a minute late.

These bugs don't show up in the query text. They don't show up in a test against fixed fixtures either. They show up when events [are replayed at their original timestamps](/blog/p3-testing-framework), which is a different kind of test than a check on a result table.

## Why it needs a different harness

For analytics engineering, a coding agent mostly needs SQL skills and context about the warehouse: which tables exist, what the columns mean, which conventions the team follows. The warehouse does the integration and the operations. The warehouse-native agents being built today handle this well.

Operational data engineering is a different problem. The hard part isn't writing the SQL. It's making several systems agree, getting the behavior over time right, and proving the guarantees a live consumer depends on. Better context doesn't solve that. A smarter model doesn't either. The agent needs feedback from tools that see what it can't.

That's a **harness**: the tools and checks an agent works inside. A compiler generates the seams between systems from one model, so they can't disagree. A validator checks keys, timestamps, and table types across the whole pipeline. A simulator replays event time so the agent can see how the pipeline behaves when data arrives late or out of order. The agent writes the logic. The harness verifies it before anything reaches a live consumer.

That's what we built [DataSQRL](/docs/intro) to be: an open-source harness for agentic data engineering on operational data. Agents build real-time pipelines, data APIs, and data products. A compiler verifies them.

## Use both

The two worlds meet in the data lake and warehouse. Operational pipelines often land their results there, for example CDC streams enriched and written to Iceberg with views for the warehouse's query engine. The split is simple. Models inside the warehouse stay in dbt. Pipelines that serve live consumers, including the ones that land in the warehouse, belong in an operational harness. Many teams run both.

Not sure which one your problem is? [How DataSQRL Compares](/docs/intro/comparison) lists the signals: a data API over operational data, Kafka in the pipeline, results needed in seconds or minutes, or applications and AI agents as consumers. If any of those apply, you're doing operational data engineering.
