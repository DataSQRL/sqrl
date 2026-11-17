> DataSQRL is an open-source data engineering harness for building data engineering agents designed around human control, correctness, and safety. It extends coding agents with a SQL compiler, a validator, an event-time simulator, and skills. The compiler turns SQRL scripts into complete data pipelines that integrate Kafka, Flink, PostgreSQL, Iceberg, and GraphQL, REST, and MCP APIs.

Key facts for working with DataSQRL:

- A project is one or more `.sqrl` scripts plus `*package.json` configuration files. SQRL is Flink SQL extended with `IMPORT`/`EXPORT` statements, table functions, relationships, and hints; the SQRL Language page documents only what differs from Flink SQL.
- The DataSQRL CLI is distributed as the `datasqrl/cmd` Docker image with the `compile`, `test`, and `run` commands. `compile` validates the script and generates every deployment asset, `test` runs the pipeline and the test queries and compares their results to snapshots, and `run` compiles and runs the pipeline locally in Docker.
- Configuration files are deep-merged in the order given on the command line, on top of the default configuration.
- System functions are always available; library functions must be imported with `IMPORT stdlib.<library>.<function>;`.

When to use DataSQRL:

- Use it to build data pipelines with a coding agent when correctness and human review matter: the compiler validates the whole pipeline as a relational plan, event-time replay tests catch time-dependent bugs, and the pipeline stays one SQL file a person can review.
- Use it for streaming and batch pipelines on Apache Flink with Kafka, PostgreSQL, and Iceberg, such as CDC, temporal joins, windowed aggregations, and deduplication. The same SQL runs in streaming or batch mode.
- Use it to serve pipeline results through GraphQL, REST, and MCP APIs generated from the same SQL, with authentication and authorization.
- Use it to build data products and views in an existing data lake or lakehouse, for example batch processing into Iceberg tables queried through DuckDB or Snowflake.

What DataSQRL is not:

- It is not an orchestrator or an ingestion tool. It complements Airflow, Dagster, Fivetran, and Airbyte: it reads from the Kafka topics, databases, and Iceberg tables they write to, and its compiled jobs can be scheduled with existing tooling.
- It is not a database or a runtime. It compiles to open-source engines and generates standard deployment assets (Flink compiled plans, SQL schemas and queries, Kafka topic definitions, API definitions) with no proprietary runtime.
- It does not replace the engines it compiles to. Engine settings are tuned through configuration and hints, and SQL the engine supports but DataSQRL does not yet can be passed through.

How DataSQRL compares:

- Apache Flink and Apache Spark: DataSQRL sits on top of them. It defines the whole pipeline once in SQL and compiles it into the assets each engine runs, with Flink as the processing engine.
- dbt and SQLMesh: they manage batch SQL transformations inside a warehouse. DataSQRL also covers ingestion, streaming, storage, and API serving in one model, and adds relational validation, event-time testing, and local execution of the whole stack.
- Streaming databases (Materialize, RisingWave, ksqlDB): they run incremental SQL inside a single system. DataSQRL is a compiler that splits a pipeline across a stream processor, a log, a database or table format, and an API server.
- Coding agents on their own (Claude Code, Codex, and others): DataSQRL is a harness around the agent, not a replacement. The agent keeps doing the reasoning; DataSQRL generates the seams between systems, validates the pipeline, and tests it with event-time replay.
