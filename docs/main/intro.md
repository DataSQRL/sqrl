# DataSQRL Documentation

DataSQRL is an open-source **data engineering harness** for building data engineering agents designed around **human control, correctness, and safety**. It extends your coding agent (Claude Code, Codex, OpenCode, Pi, and others) with a SQL compiler, a validator, an event-time simulator, and your own skills and policies. The result is an agent you can trust with data pipelines, batch jobs, data APIs (REST, GraphQL, MCP), data products, and operational data.

![DataSQRL harness: your skills and policies, a coding agent, and the DataSQRL framework packaged as one data engineering agent](https://docs.datasqrl.com/img/diagrams/agentic/harness_toolkit.svg)

## How It Works

1. **The agent writes SQL.** The whole pipeline, from ingest to transform to store to serve, is expressed in [SQRL](https://docs.datasqrl.com/docs/main/sqrl-language.md): SQL extended with stream processing and API definitions. It stays readable enough for a human to review.
2. **The compiler validates and generates.** DataSQRL checks the logical plan (schemas, keys, timestamps, table types) and the physical plan (engine capabilities, type mappings). It then generates every deployment asset from one model: Flink plans, Kafka topics, Postgres and Iceberg schemas, and GraphQL, REST, and MCP APIs.
3. **The simulator tests.** Pipelines run locally with timestamp-accurate event replay, so time-dependent behavior becomes a deterministic test.
4. **You review and deploy.** [Compilation outputs](https://docs.datasqrl.com/docs/main/compilation-output.md) such as the pipeline DAG and lineage support human review and automated policy checks. The artifacts run on open-source infrastructure you operate yourself.

For the full design, read the [harness architecture](https://docs.datasqrl.com/blog/agentic-data-engineering-harness).

## Where to Go Next

| I want to… | Go to |
|---|---|
| Try it on my own data | [Getting Started](https://docs.datasqrl.com/docs/main/intro/getting-started.md): run the basic agent in Docker, or install the plugin for Claude Code, Codex, Cursor, or Copilot |
| See what it can build | [Examples](https://docs.datasqrl.com/docs/main/intro/examples.md): data products for a retail bank, plus self-contained pipelines across many use cases |
| Compare DataSQRL to Flink, Spark, dbt, and other tools | [FAQ](https://docs.datasqrl.com/docs/main/intro/faq.md): short answers to the questions data engineers ask most |
| Understand why a harness matters | The four-part series: [broken seams](https://docs.datasqrl.com/blog/p1-broken-at-seams), [relational introspection](https://docs.datasqrl.com/blog/p2-validator-relational-introspection), [event-time testing](https://docs.datasqrl.com/blog/p3-testing-framework), and [human understanding](https://docs.datasqrl.com/blog/p4-human-understanding-one-sql-file) |
| Read and review the SQL an agent produces | [SQRL Language](https://docs.datasqrl.com/docs/main/sqrl-language.md) and [Streaming Concepts](https://docs.datasqrl.com/docs/main/intro/concepts.md) |
| Connect my data sources and sinks | [Connectors](https://docs.datasqrl.com/docs/main/connectors.md) |
| Shape the APIs and data products | [Interface](https://docs.datasqrl.com/docs/main/interface.md) |
| Choose engines and deploy | [Configuration](https://docs.datasqrl.com/docs/main/configuration.md) and [Deployment](https://docs.datasqrl.com/docs/main/deployment.md): DataSQRL Cloud, managed cloud services, or Kubernetes |
| Add custom logic | [Functions](https://docs.datasqrl.com/docs/main/functions.md): the built-in library and your own UDFs |
| Compile, test, and run from the command line | [Compiler](https://docs.datasqrl.com/docs/main/compiler.md) |
| Inspect and validate what the compiler produces | [Compilation Output](https://docs.datasqrl.com/docs/main/compilation-output.md) |
| Customize or extend the harness itself | [How DataSQRL Works](https://docs.datasqrl.com/docs/main/deepdive.md) |

## Documentation Map

**Start here**
- [Getting Started](https://docs.datasqrl.com/docs/main/intro/getting-started.md): set up the DataSQRL agent and build your first pipeline
- [Examples](https://docs.datasqrl.com/docs/main/intro/examples.md): a gallery of what DataSQRL can build
- [FAQ](https://docs.datasqrl.com/docs/main/intro/faq.md): how DataSQRL compares to other tools, and common questions

**Core concepts**
- [SQRL Language](https://docs.datasqrl.com/docs/main/sqrl-language.md): imports and exports, table functions and relationships, hints, subscriptions, and stream and state semantics
- [Connectors](https://docs.datasqrl.com/docs/main/connectors.md): ingest from and export to Kafka, databases, data lakes, and files
- [Interface](https://docs.datasqrl.com/docs/main/interface.md): generated GraphQL, REST, and MCP APIs and data product tables, and how to customize them
- [Configuration](https://docs.datasqrl.com/docs/main/configuration.md): engines, connectors, dependencies, and compiler options in `package.json`, with a page for each engine ([Flink](https://docs.datasqrl.com/docs/main/configuration-engine/flink.md), [Kafka](https://docs.datasqrl.com/docs/main/configuration-engine/kafka.md), [Postgres](https://docs.datasqrl.com/docs/main/configuration-engine/postgres.md), [Iceberg](https://docs.datasqrl.com/docs/main/configuration-engine/iceberg.md), [Iceberg query engines](https://docs.datasqrl.com/docs/main/configuration-engine/iceberg-query.md), [Vert.x](https://docs.datasqrl.com/docs/main/configuration-engine/vertx.md)) and the [default configuration](https://docs.datasqrl.com/docs/main/configuration-default.md)
- [Functions](https://docs.datasqrl.com/docs/main/functions.md): [system](https://docs.datasqrl.com/docs/main/functions-system-generated.md) and [library](https://docs.datasqrl.com/docs/main/functions-library-generated.md) functions, plus custom functions
- [Compiler](https://docs.datasqrl.com/docs/main/compiler.md): the `compile`, `test`, and `run` commands, the [compilation output](https://docs.datasqrl.com/docs/main/compilation-output.md) they produce for review and validation, and how to [deploy](https://docs.datasqrl.com/docs/main/deployment.md) it
- [Streaming Concepts](https://docs.datasqrl.com/docs/main/intro/concepts.md): time, watermarks, and other stream processing basics

**Advanced**
- [How DataSQRL Works](https://docs.datasqrl.com/docs/main/deepdive.md): internal architecture and advanced customization
- [Compatibility](https://docs.datasqrl.com/docs/main/compatibility.md): version compatibility and migration

## Community & Support

DataSQRL is [open source](https://github.com/DataSQRL/sqrl). Report bugs in [GitHub Issues](https://github.com/DataSQRL/sqrl/issues) and ask questions or share feedback in [GitHub Discussions](https://github.com/DataSQRL/sqrl/discussions/). Contributions are welcome.
