---
name: implement-sqrl
description: Use when writing or editing DataSQRL scripts (.sqrl) for data processing, transformation, aggregation, or serving logic. Use for any pipeline logic implementation. Contains SQRL language spec.
---

# Implement SQRL scripts

SQRL is an extension of Flink SQL that adds support for table functions and convenience syntax to build reactive data processing and serving applications.  
The “R” in **SQRL** stands for *Reactive* and *Relationships*.

This document focuses only on features **unique to SQRL**; when SQRL accepts Flink SQL verbatim we simply refer to the upstream spec.

## Reference Templates

Review project examples in `/opt/datasqrl-examples/README.md` before implementing new features to identify related projects and copy features from those reference implementations.

## Script Structure

A SQRL script is an **ordered list** of statements separated by semicolons (`;`).  
Only one statement is allowed per line, but a statement may span multiple lines.

Typical order of statements:

```text
IMPORT ...            -- import other SQRL scripts
CREATE TABLE ...      -- define internal & external sources
MyTable := SELECT ... -- define tables or functions (with hints)
EXPORT MyTable TO ... -- write table data to sinks
```

At compile time the statements form a *directed-acyclic graph* (DAG).  
Each node is then assigned to an enabled execution engine according to the optimizer and the compiler generates the data processing code for that engine.

## Flink SQL

SQRL inherits full Flink SQL grammar for

* `CREATE {TABLE | VIEW | FUNCTION | CATALOG | DATABASE}`
* `SELECT` queries inside any of the above
* `USE ...`
* `INSERT INTO`

SQRL currently tracks Flink 2.3; use its compatible SQL syntax. Refer to the Flink SQL documentation for the detailed specification.

## Type System
In SQRL, every table and function has a type based on how the table represents data.
The type determines the semantic validity of queries against tables and how data is processed by different engines.

SQRL assigns one of the following types to tables based on the definition:
- **STREAM**: Represents a stream of immutable records with an assigned timestamp (often referred to as the "event time"). Streams are append-only. Stream tables represent events or actions over time.
- **VERSIONED_STATE**: Contains records with a natural primary key and a timestamp, tracking changes over time to each record, thereby creating a change-stream.
- **STATE**: Similar to VERSIONED_STATE but without tracking the history of changes. Each record is uniquely identified by its natural primary key.
- **LOOKUP**: Supports lookup operations using a primary key against external data systems but does not allow further processing of the data.
- **STATIC**: For data that does not change over time, such as constants.

The table type determines what operators a table supports and how those operators are applied.

## IMPORT Statement

Imports another SQRL script into the current script.
```
IMPORT qualifiedPath (AS identifier)?;
IMPORT qualifiedPath.*;             -- wildcard
```

### Path resolution

A `qualifiedPath` resolves in exactly one of three ways:

* **Relative (default)** — against the directory of the importing script: `IMPORT my.custom.script` maps to `./my/custom/script.sqrl`.
* **`root.` (reserved)** — resolves the rest of the path against the **build root of the top-level project being compiled**, no matter how deeply nested the importing script is. SQRL dot-paths have no `..`, so a script in a **subfolder** cannot climb upward relatively; `root.` is the only way for it to reach something at the project root (`IMPORT root.data_catalog.identity.customers;`, `IMPORT root.functions.my_func;`).
* **`stdlib.` (reserved)** — resolves to compiler built-ins (e.g. `IMPORT stdlib.math.*;`), not the file system. Import a whole library with `.*` or a single function by name.


- The path may continue one segment past a SQRL script to import a **single `CREATE TABLE`** from it. Standard-library paths may similarly select a function, e.g. `IMPORT stdlib.math.hypot AS hypotenuse`. Hyphenated segments are written as they are (`IMPORT connectors.kafka-source.*`); backticking a segment is also accepted (`` IMPORT `data_catalog`.`sources` ``). A `{{...}}` template variable in the path is substituted from `script.config` before the path is resolved (`IMPORT connectors.sources-{{environment}} AS sources;` — see [Config Templating](guides/templating.md)).
- Imports that end in `.*` are imported inline which means that the statement from that script are executed verbatim in the current script.
Otherwise, imports are available within a namespace that's equal to the name of the script or the optional `AS` identifier.

### Examples

* `IMPORT my.custom.script.*`: All table definitions from the script are imported inline and can be referenced directly as `MyTable` in `FROM` clauses.
* `IMPORT my.custom.script`: Tables are imported into the `script` namespace and can be referenced as `script.MyTable`
* `IMPORT my.custom.script AS myNamespace`: Tables are imported into the `myNamespace` namespace and can be referenced as `myNamespace.MyTable`
* `IMPORT connectors.sources-{{environment}} AS sources`: Imports the connector file selected by the `environment` config variable (`sources-test.sqrl`, `sources-prod.sqrl`, …) into the `sources` namespace
* `IMPORT functions.*` / `IMPORT functions.my_func`: Imports Java UDFs from the project's `functions/` folder (invoke the `/implement-udf` skill); `IMPORT stdlib.math.*` imports built-in functions (see [functions.md](functions.md))

### Import style controls API exposure

Because a wildcard import executes the statements verbatim in the current script (above), its tables become definitions of this script and follow the normal exposure rules (see *Interfaces*). A namespaced import keeps its tables **private**: they can be referenced in `FROM` clauses as `namespace.Table`, but they are not exposed in the interface directly.

```sql
IMPORT connectors.sources AS sources;   -- private: sources.Customer usable in FROM, absent from the API directly
IMPORT connectors.enablement.*;         -- inline: these tables are exposed in the API
```

To publish only what the API needs, import the sources aliased and re-declare the tables that should be exposed:

```sql
IMPORT connectors.sources AS sources;
/** Purchases with computed line total. */
Purchase := SELECT *, quantity * unit_price AS total_price FROM sources.Purchase;
```

When a single table must be a **mutation**, put it in its own connector file and wildcard-import that file, while the read-only sources stay aliased.

### Included projects / multi-project (`script.include`)

A project can import scripts from **another SQRL project**, e.g. a shared data catalog or a metrics layer reused by several pipelines, declared under `script.include` in the project's configuration file. To include another SQRL project, invoke the `/configure-sqrl` skill for configuration. What matters for the language is how the imports resolve:

* At compile time the included project (the directory holding the top-level package file, together with all its subfolders, every `.sqrl` in them is loaded) is **copied into `build/<namespace>/`**, alongside the consumer's own scripts, where `<namespace>` is the key of its `script.include` entry in the configuration file. A script reaches it with a plain **relative** import, like `IMPORT <namespace>.sources AS sources;`, and that is the preferred form. Name include namespaces with underscores (`data_catalog`), because a dash has a different meaning in SQL. When the importing script is in a subdirectory (e.g. `./my-module/module-script.sqrl`), use `IMPORT root.<namespace>.sources;` (see *Path resolution*). Below the namespace the path follows the included project's own directory structure at any depth. Example:`IMPORT data_catalog.customer.customer_data.customer_master-{{environment}} AS cs;`.
* `{{...}}` in the *consumer's* import statement is substituted from the consumer's own `script.config`; `{{...}}` inside the *included project's* scripts (e.g. a catalog's `{{environment}}` or a `{{^is_batch}}…{{/is_batch}}` section) is rendered with the `script.config` of the package file named by `package`, overridden by `script.include.<namespace>.config`.
* **Inside an included project, reference sibling scripts with *relative* imports**, so they travel with the copied files. A `root.` import inside an included project breaks once it is consumed: `root` then points at the consumer's build root, not the included project's.

## CREATE TABLE (internal vs external)

SQRL understands the complete Flink SQL `CREATE TABLE` syntax, but distinguishes between **internal** and **external** source tables.
External source tables are standard Flink SQL tables that connect to an external data source (e.g. database or Kafka cluster).
Internal tables are managed by SQRL through an engine hint (`kafka` or `iceberg`). Kafka internal sources are exposed for data ingestion in the interface.

| Feature           | Internal source (managed by SQRL)                                   | External Source (connector)    |
|-------------------|---------------------------------------------------------------------|--------------------------------|
| Connector options | **omitted**; other engine-specific `WITH (...)` options are allowed | **required**                   |
| Engine hint       | **required**                                                        | Not used to select a connector |
| Metadata columns  | `METADATA FROM 'uuid'`, `'timestamp'` are recognised by planner     | Passed through                 |
| Watermark spec    | **generated**                                                       | **required**                   |
| Primary key       | *Unenforced* upsert semantics                                       | Same as Flink                  |

Example (internal): An internal table managed by DataSQRL as a Kafka topic carries the `engine(kafka)` hint (see the `/manage-connector` skill):

```sql
/*+engine(kafka) */
CREATE TABLE Customer (
  customerid BIGINT,
  email STRING,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  PRIMARY KEY (customerid) NOT ENFORCED
);
```

The internal table does not need `WATERMARK` declaration. SQRL generates the watermark for it. 
Declare a `WATERMARK` explicitely on an internal table only when the user asks for one.

Example (external): An external table connects to a data system through a `WITH (...)` clause and declares its own `WATERMARK`:

```sql
CREATE TABLE kafka_json_table (
  user_id    INT,
  name       STRING,
  event_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka-safe',
  'topic'     = 'users',
  'format'    = 'flexible-json'
);
```

- Invoke the `/manage-connector` skill before writing or editing any table with a `WITH (...)` clause i.e. every external source and sink. This section covers the statement syntax only. Use that skill to decide which connector and format to use, the watermark rules for the source type, whether the schema belongs in a separate `_schema` table referenced with `LIKE`, and which values become `${VARIABLE}` references.
- With the `_schema` + `LIKE` source pattern, the `WATERMARK`, any `METADATA FROM '...'` column, and any computed (`AS ...`) column belong to the **connector table**, never the `_X_schema` table. Invoke the `/manage-connector` skill for those rules.

### File-based sources: watermarks & snapshot aggregation

File-based sources (`filesystem`, `iceberg`, `hudi`, `deltalake`) behave differently from Kafka streams.

**(A) Watermark column.** A file-based source whose data timestamp is not quasi-monotonic, or whose files do not arrive frequently, watermarks a computed `ingestion_time AS now()` column instead of a data column. The one exception to "prefer event time". The rule that decides this belongs to the source table: invoke the `/manage-connector` skill (Event-Time Processing → Watermarks on production file-based sources).

**(B) Complete-snapshot sources.** If a file source re-lists every object each cycle (e.g. S3 Inventory, full dumps), a global (non-windowed) `GROUP BY` **double-counts** — it counts each object once per snapshot. Aggregate each snapshot independently with a `SESSION` window over the `ingestion_time AS now()` column from (A), `PARTITION BY` the group key (read [SESSION Windows](guides/session-tvf.md) first): each snapshot's records arrive as one ingestion burst and form one session, so they are counted once. A short session gap (e.g. 1 minute) closes the window shortly after a snapshot finishes loading, while the much larger gap between snapshots keeps them in separate sessions.

**Wrong fixes:** a `PRIMARY KEY` on a filesystem source does **not** dedup (the source is append-only); even a correct `DISTINCT`-dedup mishandles deletions (an object disappears by absence in the next snapshot) and grows unbounded state.

## Definition statements

### Table definition

```
TableName := SELECT ... ;
```

Equivalent to a `CREATE VIEW` in SQL.

```sql
ValidCustomer := SELECT * FROM Customer WHERE customerid > 0 AND email IS NOT NULL;
```

A common pattern is stream enrichment with a temporal join:

```sql
-- Temporal join for stream enrichment — intermediate result uses _ prefix
_EnrichedOrders := SELECT o.*, c.name, c.email
  FROM Orders o
  JOIN Customer FOR SYSTEM_TIME AS OF o.event_time AS c
    ON o.customerid = c.customerid;

-- If the enriched table is exposed directly in the API, use a public name:
/** Orders enriched with customer details */
EnrichedOrders := SELECT o.*, c.name, c.email
  FROM Orders o
  JOIN Customer FOR SYSTEM_TIME AS OF o.event_time AS c
    ON o.customerid = c.customerid;
```

### DISTINCT operator

```
DistinctTbl := DISTINCT SourceTbl
               ON pk_col [, ...]
               ORDER BY ts_col [ASC|DESC] [NULLS LAST] ;
```

* Deduplicates a **STREAM** of changelog data into a **VERSIONED_STATE** table.
* The SQRL `DISTINCT` statement is not SQL `SELECT DISTINCT`.
* Hint `/*+filtered_distinct_order*/` (see hints) may precede the statement to push filters before deduplication for optimization.

**Use `DISTINCT` when the source table is a `STREAM` of updates to an entity that needs to be converted to a state table, i.e. changelog.** A `DISTINCT` statement adds a stateful operator and changes the semantics of the table.
Check the type of the source table before you write one:

| Source table | What to do |
|---|---|
| A `STREAM` that carries several records for the same key, for example an append-only change feed | Write the `DISTINCT` statement where a consumer needs a single version per entity, either the current one or the one valid at an event's timestamp in a temporal join. It turns the stream into `VERSIONED_STATE`. |
| A table that already declares a `PRIMARY KEY` with upsert semantics, for example an `upsert-kafka` source or an internal table with an engine hint and a primary key | Read the table directly. It is `VERSIONED_STATE` already, so a `DISTINCT` on that same key returns the same rows and only costs state. |

Read the `STREAM` directly where a consumer needs the changes themselves, such as a history endpoint, an aggregation over every update, an alert per change, or a stream-to-stream join. When both consumers exist, keep the `STREAM` and define the `DISTINCT` table, if needed, beside it, so each keeps its own semantics.

```sql
-- Intermediate dedup step (feeds into another table) — use _ prefix
_DistinctProducts := DISTINCT Products ON id ORDER BY updated DESC;

-- Final dedup result exposed directly in the API — use public name
/** Latest version of each product, deduplicated by id */
DistinctProducts := DISTINCT Products ON id ORDER BY updated DESC;
```

### Primary key columns

A table gets a primary key from a `PRIMARY KEY` in its `CREATE TABLE`, or from a `DISTINCT ... ON` or `GROUP BY` on the key columns. Such a table has the type `VERSIONED_STATE` or `STATE` (see [Type System](#type-system)).

Clean the primary key columns before the first table that has that primary key, and select them unchanged in every table that reads from that table, directly or through other tables.
For example, when `Customer` has the primary key `customer_id`, a later table selects `customer_id`, and never `TRIM(customer_id) AS customer_id`.

A function on a key column in such a later table (`TRIM`, `LOWER`, `REGEXP_REPLACE`, `CAST`, `NULLIF`) hides the key from the Flink planner. A function can turn two different keys into the same value, so the planner treats the result as a normal column, not as a key. Every join that reads the later table stores each full row in its state and finds a row by comparing all of its columns (`NoUniqueKey`), instead of finding it by its key. This state grows with the data, and every update becomes slower.
Columns that are not part of the key can be cleaned in any table.

These rules apply to entity data (see the `/manage-connector` skill, Entity Data), where each new record replaces the previous version of the same entity, such as a customer or an account.

**Writing entity data.** When the pipeline writes entity data into a table that other tables or projects read, clean the key before the `EXPORT`.
Such a table is a mutation table with a `PRIMARY KEY` (an internal table with an engine hint) or a Kafka sink with a `PRIMARY KEY`.
Every reader then receives the final key value and can select it unchanged.
Follow these steps:

1. Clean the key in a view, and keep only the rows whose cleaned key is not empty.
2. Make the cleaned key unique with `DISTINCT ... ON` the cleaned key.
3. Export the `DISTINCT` table into the table with the primary key.

In this example, `RawCustomerUpdate` is the source table the pipeline reads. It has no key, and its `customer_id` values can contain extra spaces or be empty.

```sql
/*+engine(kafka) */
CREATE TABLE Customer (
  customer_id STRING NOT NULL,
  email STRING,
  event_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp',
  PRIMARY KEY (customer_id) NOT ENFORCED
);

_CleanCustomerUpdate := SELECT NULLIF(TRIM(REGEXP_REPLACE(customer_id, '\s+', ' ')), '') AS customer_id,
                               email, event_time
                        FROM RawCustomerUpdate
                        WHERE NULLIF(TRIM(REGEXP_REPLACE(customer_id, '\s+', ' ')), '') IS NOT NULL;

_LatestCustomerUpdate := DISTINCT _CleanCustomerUpdate ON customer_id ORDER BY event_time DESC;

EXPORT _LatestCustomerUpdate TO Customer;
```

Step 2 is required.
Flink rejects an `EXPORT` into a table with a `PRIMARY KEY` when the exported query is not unique on that key, with the error `The query has an upsert key that differs from the primary key of the sink table`.

**Reading keys that arrive dirty.** When the writer is outside this project, for example an external client that calls a mutation endpoint, the pipeline cannot clean the key before it is stored.
Clean it in the first table after the mutation instead:

1. Declare the mutation without a `PRIMARY KEY`, so that each write is stored as an event with the key value exactly as the client sent it.
   A `PRIMARY KEY` on the raw key would make the raw value the key, and cleaning it in a later table would hide that key from the planner.
2. Clean the key in a view, and keep only the rows whose cleaned key is not empty.
3. Make the cleaned key unique with `DISTINCT ... ON` the cleaned key. Every later table reads this `DISTINCT` table.

```sql
/*+engine(kafka) */
CREATE TABLE CustomerUpdate (
  customer_id STRING NOT NULL,
  email STRING,
  event_time TIMESTAMP_LTZ(3) NOT NULL METADATA FROM 'timestamp'
);

_CleanCustomerUpdate := SELECT NULLIF(TRIM(REGEXP_REPLACE(customer_id, '\s+', ' ')), '') AS customer_id,
                               email, event_time
                        FROM CustomerUpdate
                        WHERE NULLIF(TRIM(REGEXP_REPLACE(customer_id, '\s+', ' ')), '') IS NOT NULL;

/** Current version of each customer, with the cleaned id as primary key */
Customer := DISTINCT _CleanCustomerUpdate ON customer_id ORDER BY event_time DESC;
```

Follow the same steps for an external source whose keys can arrive dirty.
When an external source's keys always arrive clean, read its key unchanged and skip the cleaning.

Clean the key in a view, as shown above, and not inside the `CREATE TABLE`.
Flink accepts a `PRIMARY KEY` only on physical columns, so a computed column such as `customer_id AS TRIM(raw_customer_id)` cannot be the key.

When the compiler asks for a `/*+primary_key(...)*/` hint on a table whose key column comes from a function, use `DISTINCT ... ON` the cleaned key instead, because the hint does not make the column unique.

### Function definition

```
FuncName(arg1 TYPE [NOT NULL] [, ...]) :=
  SELECT ... WHERE col = :arg1 ;
```

Arguments are referenced with `:name` in the `SELECT` query. Argument definitions are identical to column definitions in `CREATE TABLE` statements.

```sql
CustomerByEmail(email STRING) := SELECT * FROM Customer WHERE email = :email;
```

#### Accessing JWT payload

To access the JWT payload included in the Authorization HTTP header, you can use the `METADATA` expression within the
function definition. The JWT payload is available via the auth object, and nested fields can be accessed directly.
In the example below, the JWT payload contains a val field, which is an integer.

```sql
AuthFilter(mySecretId BIGINT NOT NULL METADATA FROM 'auth.val') :=
  SELECT c.* 
  FROM Customer c 
  WHERE c.customerId = :mySecretId;
```

### Relationship definition

```
ParentTable.RelName(arg TYPE, ...) :=
  SELECT ...
  FROM Child c
  WHERE this.id = c.parentId
  [AND c.col = :arg ...] ;
```

`this.` is the alias for the parent table to reference columns from the parent row.

```sql
Customer.highValueOrders(minAmount BIGINT) := SELECT * FROM Orders o WHERE o.customerid = this.id AND o.amount > :minAmount;
```

### Column-addition statement

```
TableName.new_col := expression;
```

Must appear **immediately after** the table definition it extends, and may reference previously added columns of the same table. Cannot be applied after `CREATE TABLE` statements.


### Relation alias as ROW

A relation alias is the alias of any `FROM` item: a table, a view, a subquery or a CTE. Used by itself in a `SELECT` list, it produces one `ROW` value holding every column of that relation. The API exposes the column as a nested object. 
The compiler expands the alias to `CAST(ROW(c.col1, ...) AS ROW<...>)` from the relation's validated columns, so the alias replaces writing that cast by hand.

Examples:
```sql
TableName := SELECT o.id, c AS customer FROM Orders o JOIN Customer c ON o.customerid = c.customerid;
```

```sql
/** Orders with the customer as it was when the order was placed */
OrderWithCustomer := SELECT o.id, o.amount, o.event_time, c AS customer
  FROM Orders o
  JOIN Customer FOR SYSTEM_TIME AS OF o.event_time AS c
    ON o.customerid = c.customerid;
```

The `FOR SYSTEM_TIME AS OF` clause selects the customer version valid at `o.event_time`. The alias itself packs whichever row the join produced.

* The flink row carries every column of the relation, including its timestamp column. A `_`-prefixed column is stored inside the row and omitted from the API type.
* A column named like the alias takes precedence, and the alias then resolves to that column. Give the alias a name no column uses when you want the row.
* A bare alias, `SELECT customerid, c FROM Customer c`, names the column after the alias. `c AS customer` names it explicitly.
* The alias combines with `o.*`, works inside CTEs and subqueries, and works with `LEFT JOIN`, where the row stays non-null and every field inside it becomes nullable.
* Read a field back downstream with dot access: `SELECT x.id, x.customer.name FROM OrderWithCustomer x`.
* Copy a field out as its own column as well, `c.tier AS customer_tier`, when the API filters or sorts by it. The nested value is one JSON column in the database and carries no relationships or filter arguments of its own.

Read the [Stream Enrichment](guides/stream-enrichment.md) guide to choose between the whole-row alias, copied scalar columns, and a relationship.

### Passthrough definitions

Passthrough definitions allow you to bypass SQRL's analysis and translation, passing SQL queries directly to the underlying database engine.
This serves as a "backdoor" for SQL constructs that SQRL does not yet support natively.

Passthrough table, function, or relationship definition have a `RETURNS` clause in their signature that defines the result type of the query.

```
TableName RETURNS (column TYPE [NOT NULL], ...) := 
  SELECT raw_sql_query;
```

**⚠️ Important considerations:**
- SQL must be written in the exact syntax of the target database engine
- SQRL will not validate, parse, or optimize the query
- Only use when SQRL lacks native support for the required functionality
- Return type must be explicitly declared using the `RETURNS` clause
- Passthrough queries must be assigned to a database engine for execution

The following defines a relationship definition with a recursive CTE that is passed through to the
database engine for execution.
Use `this` to reference parent fields in a relationship definition or `:name` to reference function
arguments as in standard definitions. The only thing that changes is that adding a `RETURNS` declaration
bypasses the SQRL analysis, optimization, and query rewriting.

```sql
Employees.allReports RETURNS (employeeid BIGINT NOT NULL, name STRING NOT NULL, level INT NOT NULL) :=
  WITH RECURSIVE employee_hierarchy AS (
    SELECT r.employeeid, r.managerid, 1 as level
    FROM "Reporting" r
    WHERE r.managerid = this.employeeid
    
    UNION ALL
    
    SELECT r.employeeid, r.managerid, eh.level + 1 as level
    FROM "Reporting" r
    INNER JOIN employee_hierarchy eh ON r.managerid = eh.employeeid
  )
  SELECT e.employeeid, e.name, eh.level
  FROM employee_hierarchy eh
  JOIN "Employees" e ON eh.employeeid = e.employeeid
  ORDER BY eh.level, e.employeeid;
```

## Functions

SQRL supports the Flink 2.3-compatible built-in functions plus its own scalar, aggregate, and table functions. [Look up the required function](functions.md).

## Interfaces

The tables and functions defined in a SQRL script are exposed through an interface.
The term "interface" is used generically to describe a means by which a client, user, or external system can access the processed data.
The interface depends on the configured engines: API endpoints for servers, queries and views for databases, and topics for logs.
An interface is a sink in the data processing DAG that's defined by a SQRL script.

How a table or function is exposed in the interface depends on the access type. The access type is one of the following:

| Access type              | How to declare               | Surface                                                                                              |
|--------------------------|------------------------------|------------------------------------------------------------------------------------------------------|
| **Query** (default)      | no modifier                  | GraphQL query / SQL view / log topic (pull)                                                          |
| **Subscription**         | prefix body with `SUBSCRIBE` | GraphQL subscription / push topic                                                                    |
| **No direct query**        | `/*+no_query*/` hint         | still **in** the interface, reachable through a relationship or an explicit function but cannot be queryable directly |
| **Not in the interface but intermediate step** | object name starts with `_`  | not exposed at all                                                                                   |

Example:

```sql
HighTempAlert := SUBSCRIBE
                 SELECT * FROM SensorReading WHERE temperature > 50;
```
Defines a subscription endpoint that is exposed as a GraphQL subscription or Kafka topic depending on engine configuration.
The assignment operator comes first and the `SUBSCRIBE` keyword comes after it (`Name := SUBSCRIBE SELECT ...`).

```sql
HighTemperatures := SELECT * FROM SensorReading WHERE temperature > 50;
```
Defines a query table that is exposed as a query in the API or view in the database.

```sql
HighTemperatures(temp BIGINT NOT NULL) := SELECT * FROM SensorReading WHERE temperature > :temp;
```
Defines a query function that is exposed as a parametrized query in the API.

```sql
_HighTemps := SELECT * FROM SensorReading WHERE temperature > 50;
```
Defines an internal table that is not exposed in the interface.

### `_` prefix vs. `/*+no_query*/` — different semantics, never interchangeable

* **`_` prefix** — the object is **not part of the interface at all**: no endpoint, no generated type, unreachable from any client. This is what pipeline intermediates use (dedup, enrichment, filter and join steps).
* **`/*+no_query*/`** — the object **is** part of the interface through relationship but it cannot be queryable direclty. Use it for a table clients reach only through a relationship from its parent.

```sql
/** Purchase order lines. Reached through the relationships below, never queried directly. */
/*+no_query */
Purchase := SELECT *, quantity * unit_price AS total_price FROM sources.Purchase;
Purchase.offering := SELECT * FROM Offering o WHERE o.offering_id = this.offering_id LIMIT 1;
Purchase.customer := SELECT * FROM Customer c WHERE c.customer_id = this.customer_id LIMIT 1;

/** Explicit, parametrized access to the same table. */
Order(order_id STRING NOT NULL) := SELECT * FROM Purchase WHERE order_id = :order_id ORDER BY offering_id ASC;
```

Applying `no_query` to a `_`-prefixed table is illogical. Pick one: `_` for a pipeline intermediate, `no_query` for an interface object that must not have its own query endpoint.

### CREATE TABLE

CREATE TABLE statements that define an internal data source are exposed as topics in the log, or GraphQL mutations in the server.
The input type is defined by mapping all column types to native data types of the interface schema. Computed and metadata columns are not included in the input type since those are computed on insert.

## EXPORT statement

```
EXPORT source_identifier TO sinkPath.QualifiedName ;
```

* `sinkPath` maps to a connector table definition when present, or one of the **built-in** sinks:
    * `print.*` – stdout
    * `logger.*` – uses configured logger
    * `log.*` – topic in configured log engine

```sql
EXPORT CustomerTimeWindow TO print.TimeWindow;
EXPORT MyAlerts          TO log.AlertStream;
```

An export may also target one table in another script, for example `EXPORT CustomerSubset TO connectors.sinks.CustomerExport;`. The target must be either an external connector table or an engine-managed internal table.


## Hints

Hints live in a `/*+ ... */` comment placed **immediately before** the definition they apply to.

| Hint                        | Form                                                                       | Applies to     | Effect                                                                                                                                                                             |
|-----------------------------|----------------------------------------------------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **primary_key**             | `primary_key(col, ...)`                                                    | table          | declare PK when optimiser cannot infer                                                                                                                                             |
| **index**                   | `index(type, col [ASC\|DESC], ...)` <br/> Multiple `index(...)` can be comma-separated | table          | override automatic index selection. `type` ∈ `HASH`, `BTREE`, `PBTREE`, `TEXT`, `VECTOR_COSINE`, `VECTOR_EUCLID`; `DESC` is supported only for `BTREE` and `PBTREE`. <br />`index` *alone* disables all automatic indexes |
| **partition_key**           | `partition_key(col, ...)`                                                  | table          | define partition columns for sinks that support partitioning                                                                                                                       |
| **vector_dim**              | `vector_dim(col, 1536)`                                                    | table          | declare fixed vector length. This is required when using vector indexes.                                                                                                           |
| **query_by_all**            | `query_by_all(col, ...)`                                                   | table          | generate interface with *required* filter arguments for all listed columns                                                                                                         |
| **query_by_any**            | `query_by_any(col, ...)`                                                   | table          | generate interface with *optional* filter arguments for all listed columns                                                                                                         |
| **no_query**                | `no_query`                                                                 | table          | prevent the table from being directly queryable but reachable through relationships and explicit functions (see *Interfaces*)                        |
| **insert**                  | `insert(type)`                                                             | table          | controls the way how mutations will be written to their target sink. `type` ∈ `SINGLE` (default), `BATCH`, `TRANSACTION`                                                           |
| **ttl**                     | `ttl(duration)`                                                            | table          | specifies how long the records for this table are retained in the underlying data system before it can be discarded. Expects a duration string like `5 week`. Disabled by default. |
| **cache**                   | `cache(duration)`                                                          | table          | how long the results retrieved from this table can be cached on the server before they are refreshed. Expects a duration string like `10 seconds`. Disabled by default.            |
| **filtered_distinct_order** | flag                                                                       | DISTINCT table | eliminate updates on order column only before dedup                                                                                                                                |
| **engine**                  | `engine(engine_id)`                                                        | table          | pin execution engine (`process`, `database`, `flink`, ...)                                                                                                                         |
| **test**                    | `test` or `test(no_rows)`                                                  | table          | marks test case, only executed with `test` command.                                                                                                       |
| **workload**                | `workload`                                                                 | table          | retained as sink for DAG optimization but hidden from interface                                                                                                                    |

Multiple hints are comma-separated inside one comment. This example configures a primary key and a vector index for the `SensorTempByHour` table:

```sql
/*+primary_key(sensorid, time_hour), index(VECTOR_COSINE, embedding) */
SensorTempByHour := SELECT ... ;
```

For choosing between `query_by_all` (required filter arguments) and `query_by_any` (optional filter arguments) when designing table access, invoke the `/design-api` skill (Table Hints for Table Access).


### Constraints and mutual exclusions

**Never combine `/*+no_query*/` with a `_`-prefixed name.** They are used for different purposes (see *Interfaces*): `_` removes the object from the interface entirely and it is used for intermediate processes. On the other hand, table with `no_query` hint cannot be queried directly while is accessible through relationship from other tables.

**`query_by_all` / `query_by_any` need a queryable table.** They generate interface filter arguments, so they are meaningless on a `_`-prefixed table.

**`no_query` on a `VERSIONED_STATE` (DISTINCT) tables used as temporal-join sources is illegal:**
`/*+no_query*/` makes the table unqueryable, which breaks `FOR SYSTEM_TIME AS OF` temporal join lookups. Never apply `no_query` to a table that is referenced in a temporal join.

### Testing

A `/*+test */` hint in front of a table definition marks a test case: the table is only executed by the `test` command (ignored otherwise), and its result is snapshotted in the configured `snapshot-folder`. Invoke the `/test-sqrl` skill (Writing Tests) for what a test table must satisfy (deterministic `ORDER BY`, predictable columns, the `_`-alias for sort-only columns) and for running tests and snapshots.

## NEXT_BATCH

```sql
NEXT_BATCH;
```

For batch pipelines, use `NEXT_BATCH` to split processing into sequential sub-batches; the next batch runs only when the prior one succeeds. Current SQRL code permits `NEXT_BATCH` only when `enabled-engines` is exactly `["flink"]`.

The batch allocation only applies to `EXPORT .. TO ..` statements. All interfaces are computed in the last batch.

In the following example, the `NEXT_BATCH` guarantees that the `PreprocessedData` is written completely to the sink before processing continues in the next sub-batch.

```sql
PreprocessedData := SELECT ...;
EXPORT PreprocessedData TO PreprocessorSink;
NEXT_BATCH;
...continue processing...
```

:::warning
Sub-batches are executed stand-alone, meaning each sub-batch reads the data from source and not from the intermediate results of the previous sub-batch.
If you wish to start with those, you need to explicitly write them out and read them.
:::

## Pipeline design rules
- Do not combine event time with processing time. Prefer event time for consistency and reproducible results. (Exception: file-based sources may use an ingestion-time `now()` column as the watermark — invoke the `/manage-connector` skill and see Event-Time Processing section.)
- Use Flink 2.3-compatible SQL syntax and table-valued functions (e.g. for time windows)
- Execute as much processing as possible on append-only `STREAM` tables
- Use temporal joins (`FOR SYSTEM_TIME AS OF`) and interval joins when possible
- Move stateful operations (global aggs, inner joins) to end of pipeline
- Use Flink time window aggregation when requirements specify time intervals (e.g. "aggregate x by day")

```sql
SensorAvg := SELECT sensorid, 
  TUMBLE_START(event_time, INTERVAL '1' HOUR) AS time_hour,
  AVG(temperature) AS avg_temp
FROM SensorReading
GROUP BY sensorid, TUMBLE(event_time, INTERVAL '1' HOUR);
```

## Comments & Doc-strings

* `--` single-line comment; write it on its own line, never trailing at the end of a statement
* `/* ... */` multi-line comment
* `/** ... */` **doc-string**: attached to the next definition and propagated to generated API docs.

## Validation rules

The following produce compile time errors:

* Reserved SQL keywords used as identifiers without escaping as column names (`value`, `timestamp`, `time`, `date`, `upper`, `lower`, `from`) or as table names (`Order`, `Group`, `Value`, `Key`, `Table`, `Index`, `Row`, `User`, `Transaction`, `Schema`). Use descriptive alternatives instead: `event_value`, `event_timestamp`, `upper_bound`; `ClinicalOrder`, `PatientGroup`, `AccountTransaction`.
* Duplicate identifiers (tables, functions, relationships).
* Overloaded functions (same name, different arg list) are **not** allowed.
* Argument list problems (missing type, unused arg, unknown type).
* `DISTINCT` must reference existing columns; `ORDER BY` column(s) must be monotonically increasing.
* An `ORDER BY` column that does not also appear in the `SELECT` list — in any table definition, including `/*+test*/` tables (`All sort columns must be part of the SELECT clause`. The `/test-sqrl` skill shows how to sort by a column you do not want in the result).
* Basetable inference failure for relationships.
* Invalid or malformed hints (unknown name, wrong delimiter).

## Additional pattern guides
For additional patterns, read these guides:
* [Stream Enrichment](guides/stream-enrichment.md): How to enrich a data stream with dimensional information or state consistently, and how to carry the whole dimension row as one nested object.
* [Config Templating](guides/templating.md): How to use variables in SQRL scripts for templating that gets substituted at compile time with mustache engine.
* [SESSION Windows](guides/session-tvf.md): The `SESSION` TVF for window aggregation has limitations and quirks. Always read this guide before implementing `SESSION` windows.
