---
name: manage-connector
description: Use when connecting to external data systems for ingestion or export in DataSQRL. Use for Kafka, filesystem, database, Iceberg, or other connector configuration.
---

## Creating External Connectors

Use `CREATE TABLE` statements with `WITH` clause to connect external data sources and sinks:

```sql
CREATE TABLE MyTable (
    column1 TYPE,
    column2 TYPE,
    event_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'connector-name',
    'option1' = 'value1',
    ...
);
```

**Formatting rules — always follow, never compress onto a single line:**
- `CREATE TABLE name (` on its own line; each column definition on its own line with 2-space indent; `) WITH (` on its own line
- Each connector property goes on its own line inside `WITH (`, indented 4 spaces, with spaces around `=` (e.g. `'connector' = 'kafka-safe'`)
- Connector options that apply to only one runtime mode (streaming vs. batch) go inside conditional mustache blocks — `{{#is_batch}} … {{/is_batch}}` for batch-only options, `{{^is_batch}} … {{/is_batch}}` for streaming-only ones; `{{#is_batch}}` / `{{/is_batch}}` each go on their own line with no leading indent
- Separate each `CREATE TABLE` block with a blank line when multiple tables appear in the same file

## Connector Sources

**Before writing or editing a `WITH (...)` clause, every time, fully read the linked documentation of the connector you are about to use for the connector specific configuration options, even when you are copying a `CREATE TABLE` that already exists in the project.**

* [kafka and kafka-safe](connectors/kafka.md): Read or write append-only streams from Kafka-compatible data sources. Prefer the `-safe` version which adds [DLQ and smart watermark support](connectors/kafka-safe.md).
* [upsert-kafka and upsert-kafka-safe](connectors/upsert-kafka.md): Read or write change streams for Kafka-compatible data sources. Prefer the `-safe` version which adds [DLQ support](connectors/kafka-safe.md).
* [filesystem](connectors/filesystem.md): Read or write data from local and cloud storage systems.
* [iceberg](connectors/iceberg.md): Read or write data from Apache Iceberg.
* [jdbc](connectors/jdbc.md): Write data via JDBC or use for lookup joins.
* [datagen](connectors/datagen.md): Generate synthetic data for testing.
* [print](connectors/print.md): Print output to stdout for debugging.
* [blackhole](connectors/blackhole.md): Discards all output (useful for testing).

CDC Connectors:
* [mysql-cdc](connectors/mysql-cdc.md), [postgres-cdc](connectors/postgres-cdc.md), and [sqlserver-cdc](connectors/sqlserver-cdc.md): Read CDC change streams from MySQL, PostgreSQL, and Microsoft SQL Server. These Flink CDC connectors are included by default.

If the user requires connection to other data systems, instruct the user to contact customer support. Never suggest connectors not listed above. 

## Connector Formats

**Always fully read the linked documentation of the format you are about to use for the format specific configuration options as well**. The format specific options (`*.timestamp-format.standard`, nesting support, delimiters, schema-registry settings) appear only on the format page. Connectors and formats are based on Flink 2.3.x but DataSQRL extends those with useful features (like the `-safe` kafka connector variants).

The following formats are supported.

Popular formats:
* [flexible-json and json](formats/json.md): JSON-line format. Prefer `flexible-json` since it supports nested-json in export and is otherwise identical to `json`.
* [avro](formats/avro.md): Avro format.
* [avro-confluent](formats/avro-confluent.md): Avro with Confluent schema registry.
* [csv](formats/csv.md): Delimited file format. Best for **test data** — Flink's CSV support is limited; prefer parquet/avro/flexible-json for production where practical.
* [parquet](formats/parquet.md): Apache Parquet columnar format.
* [raw](formats/raw.md): Raw byte-based format for single column values.
* [protobuf](formats/protobuf.md): Protocol Buffers format.
* [orc](formats/orc.md): Apache ORC columnar format.

CDC formats:
* [canal](formats/canal.md): Canal CDC format for streaming changes from MySQL.
* [debezium](formats/debezium.md): Debezium CDC format for streaming changes from MySQL, PostgreSQL, Oracle, SQL Server.
* [maxwell](formats/maxwell.md): Maxwell CDC format for streaming changes from MySQL.
* [ogg](formats/ogg.md): Oracle GoldenGate CDC format for real-time data replication.


## Connector Organization

**Best Practice:**
- Place all `CREATE TABLE` statements for one data source in a single `.sqrl` file
- Store in `connectors/` folder (e.g., `connectors/kafka-source.sqrl`)
- Import into main script: `IMPORT connectors.kafka-source.*;`
- **Reuse what already exists:** if a source or sink is already defined as a `.sqrl` script in the project, import that script — never copy its `CREATE TABLE` into a new file. Treat an existing definition as correct and complete unless a compile or test run actually fails on it.
- **Shared connectors:** source/sink definitions reused across projects (e.g. a shared data catalog) live in a **separate** SQRL project declared under configuration file( See how to set configure file to use shared project by invoking `/configure-sqrl` skill and `/implement-sqrl` skill for IMPORT Statement section in sqrl-language spec). Invoke the `/build-catalog` skill when building such a catalog.

## Environment Variables in Connector Config

Connector properties may reference environment variables. There are two forms, and the difference is security-relevant:

- `'${VARIABLE_NAME}'` is a **non-secret** variable. `sqrl compile` resolves it when it is available and writes the value into the `build/` artifacts (a missing one is left as a placeholder for the runtime). Use it for bootstrap servers, hostnames, ports, connection strings (e.g. `'${CUSTOMER_DATA_KAFKA_BROKERS}'`), the consumer group / deployment id (`'${DEPLOYMENT_ID}'`) and the test-data path (`'${DATA_PATH}'`).
- `'${{VARIABLE_NAME}}'` is a **secret** variable. It is never resolved at compile time; the compiler rewrites it to `${VARIABLE_NAME}` in the generated artifacts so the value is read only when the pipeline runs. Use it for **every credential**: passwords, tokens, API keys, SASL/JAAS strings (e.g. `'password' = '${{POSTGRES_PASSWORD}}'`). A secret written as `${VAR}` ends up in plain text in the build output, whatever the variable is called.

Use variables **only** for values that genuinely differ per deployment.

**Hardcode logical names** — Kafka topics, table names, stream names (e.g. `'topic' = 'customer_master.customer'`) are normally identical across environments and are not security sensitive, so a variable obfuscates the value.

## Critical Requirements

**Event-Time Processing:**
- **ALWAYS** define watermarks on external sources used for streaming projects. Prefer event-time; use processing time only for the file-based-source exception below.
- **Internal tables carry no `WATERMARK` definition.** An internal table is a table with an engine hint like `/*+engine(kafka) */` and no `'connector'` option. DataSQRL manages that table and generates its watermark. Write a `WATERMARK` on such a table only when it is requested. Invoke the `/implement-sqrl` skill (CREATE TABLE internal vs external) for the full comparison.
- Watermark timestamps must be monotonically increasing with some bounded out-of-orderedness. If no such timestamp exists on source data, define an additional column `ingestion_time AS now()` and watermark on it. This can be useful for file-based sources.
- Define watermark: `WATERMARK FOR ts AS ts - INTERVAL '1' SECOND` where the interval is an upper limit of out-of-orderedness.
- Add a primary key (`PRIMARY KEY (...) NOT ENFORCED`) and/or a partition key only if they genuinely apply to the data source/sink.

**Watermarks on production file-based sources** (`filesystem` and Iceberg sources): use a data/metadata timestamp column as the `WATERMARK` **only if BOTH** (1) it is a quasi-monotonically increasing timestamp **and** (2) new files arrive frequently and deterministically (every few minutes up to ~1 hour). Otherwise — a non-monotonic column (e.g. S3 `last_modified`, which makes the watermark jump backwards) or infrequent/irregular file drops (which make it freeze) — add a computed ingestion-time column and watermark that instead:
```sql
ingestion_time AS now(),
WATERMARK FOR ingestion_time AS ingestion_time - INTERVAL '1' SECOND
```
This is the one sanctioned exception to "prefer event time"; it applies only to file-based sources whose data timestamp fails the requirements (1) or (2) defined above. Kafka and other streaming sources keep using their source/metadata timestamp.

This rule selects the watermark of a production source. A test connector reads from the filesystem but stands in for the production source, so it does NOT pick its watermark from this rule — it mirrors whatever the production connector does (see **Test connectors mirror production** below).

**Table Types:**
- **STREAM**: Append-only (e.g., `filesystem`, `kafka-safe`, `iceberg`). The planner defaults an unclassified connector to this type for imports.
- **VERSIONED_STATE**: Retraction stream (e.g., `upsert-kafka-safe`, `mysql-cdc`, `postgres-cdc`, `sqlserver-cdc`).
- **LOOKUP**: External lookup table (e.g., `jdbc`, `jdbc-sqrl`).

**Kafka source watermarks:** `engines.kafka.use-source-watermark` and `use-transaction-source-watermark` apply only to mutation tables with a `timestamp` metadata column and require `kafka-safe` or `upsert-kafka-safe`. They generate `SOURCE_WATERMARK()`; use a normal table watermark for other connector sources.

**Entity Data:** ingest entities as a stream of updates (append-only connector). Converting to versioned state is a downstream decision made per consumer: deduplicate with `DISTINCT` where a consumer needs a single version per entity, either the current one or the one valid at an event's timestamp in a temporal join, and read the `STREAM` where a consumer needs the changes themselves, such as a history view or an aggregation over every update. A source that already declares a `PRIMARY KEY` with upsert semantics is versioned state already, so it is read directly with no `DISTINCT`. Invoke the `/implement-sqrl` skill (DISTINCT operator) for the decision.

## Connector Definitions

### File layout

```
<project>/
└── connectors/
    ├── sources.sqrl          # schema tables ONLY — one `_X_schema` per source, shared by every environment
    ├── sources-test.sqrl     # connector tables for tests — filesystem over test-data/
    ├── sources-local.sqrl    # connector tables for local runs — e.g. DataSQRL-managed Kafka
    ├── sources-prod.sqrl     # connector tables for production — Kafka, Iceberg, JDBC, …
    └── test-data/
        └── <table>.jsonl     # records the test connectors read, as '${DATA_PATH}/<table>.jsonl'
```

- **`sources.sqrl`** declares only schema tables (`_X_schema`): the physical payload columns and their doc-strings. No connector, no `WITH (...)`.
- **`sources-<env>.sqrl`** opens with `IMPORT sources.*;` and declares one connector table per source, each with its own `WITH (...) LIKE _X_schema` that defines the connector and extends the schema table. **The table name is identical in every environment** (`CustomerUpdates` in test, local and prod alike, only the connector behind it differs).
- The main script imports exactly one of these files: `IMPORT connectors.sources-{{environment}} AS sources;`, where `{{environment}}` comes from `script.config.environment` in that environment's package.json overlay.
- **`test-data/`** holds the records the test connectors read. Invoke the `/test-sqrl` skill for what those records must contain.

A source's payload columns are written once, in `_X_schema`, and shared by every environment. What each environment file adds is its own `WITH (...)` config, and its own event-time watermark declaration where the timestamp may come from a different place.

### Column ownership

A connector table takes its payload columns from a **base** through `LIKE`. Normally that base is the source's schema table (`LIKE _X_schema`). DataSQRL can also extract the table schema from a schema file (`` LIKE `user.avsc` ``) if the schema is defined externally — see *Schema Loading with LIKE Clause* below. Either way the split is the same:

| Where | What goes there |
|-------|-----------------|
| **The base** — `_X_schema`, or the schema/data file named in `LIKE` | **Physical payload columns common to every environment** — including plain business timestamps such as `source_timestamp`. No `WATERMARK`, no `METADATA FROM`, no computed (`AS ...`) column. |
| **The connector table** — the `CREATE TABLE ... LIKE` body | Only what the base does not have: the `WATERMARK`, any `METADATA FROM '...'` column, any computed (`AS ...`) column — **plus** the `WITH (...)` connector config. |
| **The data file** — `test-data/<table>.jsonl`, read through `'path' = '${DATA_PATH}/...'` (file-based connectors only) | A value for every payload column of the base, **plus** the event-time column the connector table declares as physical — the one `_X_schema` deliberately does not have. See *Test connectors mirror production*. |

**Never re-declare a column the base already defines:** duplicates result in build error.

### The event-time column

The **event-time column the watermark is built on** is declared in the connector table. Its provenance differs per environment.
Cases (a) to (c) below are external tables, and each one declares its own watermark. Case (d) is an internal table, and DataSQRL generates its watermark.

```sql
-- schema table: physical payload columns only (no WATERMARK / METADATA / AS ...)
CREATE TABLE _MyStream_schema (
  key_col   STRING NOT NULL,
  value_col DOUBLE NOT NULL
);

-- (a) External Kafka: event time comes from the record's metadata timestamp
CREATE TABLE MyStream (
  event_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
    'connector' = 'kafka-safe',
    'topic' = 'my_stream_kafka_topic',
    'properties.bootstrap.servers' = '${MY_KAFKA_BROKERS}',
    'value.format' = 'flexible-json'
) LIKE _MyStream_schema;

-- (b) Filesystem: event time is a PHYSICAL column present in the data file
CREATE TABLE MyStream (
  event_time TIMESTAMP_LTZ(3) NOT NULL,
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'filesystem', ...) LIKE _MyStream_schema;

-- (c) File source with no usable event time: COMPUTED ingestion time
CREATE TABLE MyStream (
  ingest_time AS NOW(),
  WATERMARK FOR ingest_time AS ingest_time - INTERVAL '1' SECOND
) WITH ('connector' = 'filesystem', ...) LIKE _MyStream_schema;

-- (d) Internal Kafka topic managed by DataSQRL: no connector and no WATERMARK
/*+engine(kafka) */
CREATE TABLE MyStream (
  event_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp'
) LIKE _MyStream_schema;
```

### Test connectors mirror production

A `-test` connector is a filesystem stand-in for the production source. It reproduces whatever the production connector does:

- **Production takes the timestamp from source metadata** (a Kafka record header, an Iceberg
  `source_watermark()`) — case (a) above. Declare the **same-named** column in the test connector as a plain physical timestamp, case (b), and put that field in every test record.
- **Production computes an ingestion time with `now()`**, case (c) above. Do the same in the test connector.

Rules:

- Use the same watermark column name in the test and prod connector for a given table.
- Expect the test records to carry a field `_X_schema` does not declare: the event-time column is declared in the connector table, so the records must supply it.
- Use `now()` in a test connector **only if** production does: wall-clock time makes test results non-deterministic and never exercises the production event-time path the test data exists to cover.
- Test data with no timestamp: add the field to the test data.
- Production with no usable event time (append-only object stores, full dumps): both connectors use the computed ingestion time. See **Watermarks on production file-based sources** for the two conditions that decide this, and the `/implement-sqrl` skill (File-based sources) for aggregating such a source without double-counting.

## Schema Loading with LIKE Clause

**From Avro Schema Files:**
```sql
CREATE TABLE User (
  last_updated TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  WATERMARK FOR last_updated AS last_updated - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'users',
  'format' = 'avro-confluent'
) LIKE `user.avsc`;
```
- Automatically loads column definitions from Avro schema file
- Add metadata columns and watermarks separately
- The file name after `LIKE` is an identifier, so it must be **backtick-quoted** (`` LIKE `user.avsc` ``, `` LIKE `testdata/customers.jsonl` ``). Single quotes (`LIKE 'user.avsc'`) are a parse error.

**From JSONL or CSV data files:** `` LIKE `data.jsonl` `` or `` LIKE `data.csv` `` infers the columns and configures a filesystem source. Add a `WATERMARK` in the table definition and set `'source.monitor-interval'` only when the source must continuously discover files; omit it for bounded reads.

## Connector Preferences

- Use Kafka for data sources with millisecond to seconds latency requirements, Apache Iceberg otherwise.
- Use `kafka-safe` instead of `kafka` for error handling with dead-letter queues (and `upsert-kafka-safe` instead of `upsert-kafka`)
- Use `flexible-json` instead of `json` to support nested json
- Prefer CSV for test data only (Flink's CSV support is limited); for production sources prefer `parquet`, `avro`, or `flexible-json` where practical. On any CSV source set `'csv.ignore-parse-errors' = 'true'` to survive ragged/user-provided rows — see [formats/csv.md](formats/csv.md)
