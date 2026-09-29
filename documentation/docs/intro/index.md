# DataSQRL Documentation

DataSQRL is an open-source **data engineering harness** for building data engineering agents designed around **human control, correctness, and safety**. It extends your coding agent (Claude Code, Codex, OpenCode, Pi, and others) with a SQL compiler, a validator, an event-time simulator, and your own skills and policies. The result is an agent you can trust with data pipelines, batch jobs, data APIs (REST, GraphQL, MCP), data products, and operational data.

<img src="/img/diagrams/agentic/harness_toolkit.svg" alt="DataSQRL harness: your skills and policies, a coding agent, and the DataSQRL framework packaged as one data engineering agent" width="100%" />

## How It Works

1. **The agent writes SQL.** The whole pipeline, from ingest to transform to store to serve, is expressed in [SQRL](../sqrl-language): SQL extended with stream processing and API definitions. It stays readable enough for a human to review.
2. **The compiler validates and generates.** DataSQRL checks the logical plan (schemas, keys, timestamps, table types) and the physical plan (engine capabilities, type mappings). It then generates every deployment asset from one model: Flink plans, Kafka topics, Postgres and Iceberg schemas, and GraphQL, REST, and MCP APIs.
3. **The simulator tests.** Pipelines run locally with timestamp-accurate event replay, so time-dependent behavior becomes a deterministic test.
4. **You review and deploy.** Compile outputs such as the pipeline DAG and lineage support human review and automated policy checks. The artifacts run on open-source infrastructure you operate yourself.

For the full design, read the [harness architecture](/blog/agentic-data-engineering-harness).

## Where to Go Next

| I want to… | Go to |
|---|---|
| Try it on my own data | [Getting Started](getting-started): run the basic agent in Docker, or install the plugin for Claude Code, Codex, Cursor, or Copilot |
| See what it can build | [Examples](examples): data products for a retail bank, plus self-contained pipelines across many use cases |
| Compare DataSQRL to Flink, Spark, dbt, and other tools | [FAQ](faq): short answers to the questions data engineers ask most |
| Understand why a harness matters | The four-part series: [broken seams](/blog/p1-broken-at-seams), [relational introspection](/blog/p2-validator-relational-introspection), [event-time testing](/blog/p3-testing-framework), and [human understanding](/blog/p4-human-understanding-one-sql-file) |
| Read and review the SQL an agent produces | [SQRL Language](../sqrl-language) and [Streaming Concepts](concepts) |
| Connect my data sources and sinks | [Connectors](../connectors) |
| Shape the APIs and data products | [Interface](../interface) |
| Choose engines and deploy | [Configuration](../configuration) and [Deployment Configuration](../configuration-engine/cloud-deployment) |
| Add custom logic | [Functions](../functions): the built-in library and your own UDFs |
| Compile, test, and run from the command line | [Compiler](../compiler) |
| Customize or extend the harness itself | [How DataSQRL Works](../deepdive) |

## Documentation Map

**Start here**
- [Getting Started](getting-started): set up the DataSQRL agent and build your first pipeline
- [Examples](examples): a gallery of what DataSQRL can build
- [FAQ](faq): how DataSQRL compares to other tools, and common questions

**Core concepts**
- [SQRL Language](../sqrl-language): imports and exports, table functions and relationships, hints, subscriptions, and stream and state semantics
- [Connectors](../connectors): ingest from and export to Kafka, databases, data lakes, and files
- [Interface](../interface): generated GraphQL, REST, and MCP APIs and data product tables, and how to customize them
- [Configuration](../configuration): engines, connectors, dependencies, and compiler options in `package.json`, with a page for each engine ([Flink](../configuration-engine/flink), [Kafka](../configuration-engine/kafka), [Postgres](../configuration-engine/postgres), [Iceberg](../configuration-engine/iceberg), [Iceberg query engines](../configuration-engine/iceberg-query), [Vert.x](../configuration-engine/vertx)) and the [default configuration](../configuration-default)
- [Functions](../functions): [system](../functions-system-generated) and [library](../functions-library-generated) functions, plus custom functions
- [Compiler](../compiler): the `compile`, `test`, and `run` commands and what each one produces
- [Streaming Concepts](concepts): time, watermarks, and other stream processing basics

**Advanced**
- [How DataSQRL Works](../deepdive): internal architecture and advanced customization
- [Compatibility](../compatibility): version compatibility and migration

## Community & Support

DataSQRL is [open source](https://github.com/DataSQRL/sqrl). Report bugs in [GitHub Issues](https://github.com/DataSQRL/sqrl/issues) and ask questions or share feedback in [GitHub Discussions](https://github.com/DataSQRL/sqrl/discussions/). Contributions are welcome.
