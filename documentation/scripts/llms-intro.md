> DataSQRL is an open-source data engineering harness for building data engineering agents designed around human control, correctness, and safety. It extends coding agents with a SQL compiler, a validator, an event-time simulator, and skills. The compiler turns SQRL scripts into complete data pipelines that integrate Kafka, Flink, PostgreSQL, Iceberg, and GraphQL, REST, and MCP APIs.

Key facts for working with DataSQRL:

- A project is one or more `.sqrl` scripts plus `*package.json` configuration files. SQRL is Flink SQL extended with `IMPORT`/`EXPORT` statements, table functions, relationships, and hints; the SQRL Language page documents only what differs from Flink SQL.
- The DataSQRL CLI is distributed as the `datasqrl/cmd` Docker image with the `compile`, `test`, and `run` commands. `compile` validates the script and generates every deployment asset, `test` runs the pipeline and the test queries and compares their results to snapshots, and `run` compiles and runs the pipeline locally in Docker.
- Configuration files are deep-merged in the order given on the command line, on top of the default configuration.
- System functions are always available; library functions must be imported with `IMPORT stdlib.<library>.<function>;`.
