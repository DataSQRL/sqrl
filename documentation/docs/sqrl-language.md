# SQRL Language Specification

SQRL is an extension of FlinkSQL that adds support for table functions and convenience syntax to build reactive data processing and serving applications.  
The “R” in **SQRL** stands for *Reactive* and *Relationships*.

This document focuses only on features **unique to SQRL**; when SQRL accepts FlinkSQL verbatim we simply refer to the [upstream spec](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/).

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

## FlinkSQL

SQRL inherits full FlinkSQL grammar for

* `CREATE {TABLE | VIEW | FUNCTION | CATALOG | DATABASE}`
* `SELECT` queries inside any of the above
* `USE ...`
* `INSERT INTO`

...with the caveat that SQRL currently tracks **Flink 1.19**; later features may not parse.

Refer to the [FlinkSQL documentation](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/) for a detailed specification.

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

```
IMPORT qualifiedPath (AS identifier)?;
IMPORT qualifiedPath.*;             -- wildcard
```

Imports another SQRL script into the current script. The `qualifiedPath` is a `.` separated path that maps to the local file system relative to the current script, e.g. `IMPORT my.custom.script` maps to the relative path `./my/custom/script.sqrl`.

Imports that end in `.*` are imported inline which means that the statement from that script are executed verbatim in the current script.
Otherwise, imports are available within a namespace that's equal to the name of the script or the optional `AS` identifier.

Examples:
* `IMPORT my.custom.script.*`: All table definitions from the script are imported inline and can be referenced directly as `MyTable` in `FROM` clauses.
* `IMPORT my.custom.script`: Tables are imported into the `script` namespace and can be referenced as `script.MyTable`
* `IMPORT my.custom.script AS myNamespace`: Tables are imported into the `myNamespace` namespace and can be referenced as `myNamespace.MyTable`

## CREATE TABLE (internal vs external)

SQRL understands the complete FlinkSQL `CREATE TABLE` syntax, but distinguishes between **internal** and **external** source tables.
External source tables are standard FlinkSQL tables that connect to an external data source (e.g. database or Kafka cluster).
Internal tables connect to a data source that is managed by SQRL (depending on the configured `log` engine, e.g. a Kafka topic) and exposed for data ingestion in the interface.

| Feature                       | Internal source (managed by SQRL)                               | External Source (connector) |
|-------------------------------|-----------------------------------------------------------------|-----------------------------|
| Connector clause `WITH (...)` | **omitted**                                                     | **required**                |
| Metadata columns              | `METADATA FROM 'uuid'`, `'timestamp'` are recognised by planner | Passed through              |
| Watermark spec                | **generated**                                                   | **required**                |
| Primary key                   | *Unenforced* upsert semantics                                   | Same as Flink               |

Example (internal):

```sql
CREATE TABLE Customer (
  customerid BIGINT,
  email      STRING,
  _uuid      STRING NOT NULL METADATA FROM 'uuid',
  ts         TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  PRIMARY KEY (customerid) NOT ENFORCED
);
```

Example (external):

```sql
CREATE TABLE kafka_json_table (
  user_id INT,
  name    STRING
) WITH (
  'connector' = 'kafka-safe',
  'topic'     = 'users',
  'format'    = 'flexible-json'
);
```

## Definition statements

### Table definition

```
TableName := SELECT ... ;
```

Equivalent to a `CREATE VIEW` in SQL.

```sql
ValidCustomer := SELECT * FROM Customer WHERE customerid > 0 AND email IS NOT NULL;
```

### DISTINCT operator

```
DistinctTbl := DISTINCT SourceTbl
               ON pk_col [, ...]
               ORDER BY ts_col [ASC|DESC] [NULLS LAST] ;
```

* Deduplicates a **STREAM** of changelog data into a **VERSIONED_STATE** table.
* Hint `/*+filtered_distinct_order*/` (see [hints](#hints)) may precede the statement to push filters before deduplication for optimization.

```sql
DistinctProducts := DISTINCT Products ON id ORDER BY updated DESC;
```

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

## Interfaces

The tables and functions defined in a SQRL script are exposed through an interface.
The term "interface" is used generically to describe a means by which a client, user, or external system can access the processed data.
The interface depends on the [configured engines](configuration.md#engines-enabled-engines): API endpoints for servers, queries and views for databases, and topics for logs.
An interface is a sink in the data processing DAG that's defined by a SQRL script.

How a table or function is exposed in the interface depends on the access type. The access type is one of the following:

| Access type         | How to declare                                        | Surface                                     |
|---------------------|-------------------------------------------------------|---------------------------------------------|
| **Query** (default) | no modifier                                           | GraphQL query / SQL view / log topic (pull) |
| **Subscription**    | prefix body with `SUBSCRIBE`                          | GraphQL subscription / push topic           |
| **None**            | object name starts with `_` *or* `/*+no_query*/` hint | not exposed                                 |

Example:

```sql
HighTempAlert := SUBSCRIBE
                 SELECT * FROM SensorReading WHERE temperature > 50;
```
Defines a subscription endpoint that is exposed as a GraphQL subscription or Kafka topic depending on engine configuration.

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

### CREATE TABLE

CREATE TABLE statements that define an [internal data source](#create-table-internal-vs-external) are exposed as topics in the log, or GraphQL mutations in the server.
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


## Hints

Hints live in a `/*+ ... */` comment placed **immediately before** the definition they apply to.

| Hint                        | Form                                                                       | Applies to     | Effect                                                                                                                                                                             |
|-----------------------------|----------------------------------------------------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **primary_key**             | `primary_key(col, ...)`                                                    | table          | declare PK when optimiser cannot infer                                                                                                                                             |
| **index**                   | `index(type, col, ...)` <br/> Multiple `index(...)` can be comma-separated | table          | override automatic index selection. `type` ∈ `HASH`, `BTREE`, `TEXT`, `VECTOR_COSINE`, `VECTOR_EUCLID`. <br />`index` *alone* disables all automatic indexes                       |
| **partition_key**           | `partition_key(col, ...)`                                                  | table          | define partition columns for sinks that support partitioning                                                                                                                       |
| **vector_dim**              | `vector_dim(col, 1536)`                                                    | table          | declare fixed vector length. This is required when using vector indexes.                                                                                                           |
| **query_by_all**            | `query_by_all(col, ...)`                                                   | table          | generate interface with *required* filter arguments for all listed columns                                                                                                         |
| **query_by_any**            | `query_by_any(col, ...)`                                                   | table          | generate interface with *optional* filter arguments for all listed columns                                                                                                         |
| **no_query**                | `no_query`                                                                 | table          | hide from interface                                                                                                                                                                |
| **insert**                  | `insert(type)`                                                             | table          | controls the way how mutations will be written to their target sink. `type` ∈ `SINGLE` (default), `BATCH`, `TRANSACTION`                                                           |
| **ttl**                     | `ttl(duration)`                                                            | table          | specifies how long the records for this table are retained in the underlying data system before it can be discarded. Expects a duration string like `5 week`. Disabled by default. |
| **cache**                   | `cache(duration)`                                                          | table          | how long the results retrieved from this table can be cached on the server before they are refreshed. Expects a duration string like `10 seconds`. Disabled by default.            |
| **filtered_distinct_order** | flag                                                                       | DISTINCT table | eliminate updates on order column only before dedup                                                                                                                                |
| **engine**                  | `enigne(engine_id)`                                                        | table          | pin execution engine (`process`, `database`, `flink`, ...)                                                                                                                         |
| **test**                    | `test`                                                                     | table          | marks test case, only executed with [`test` command](compiler#test-command).                                                                                                       |
| **workload**                | `workload`                                                                 | table          | retained as sink for DAG optimization but hidden from interface                                                                                                                    |

This example configures a primary key and vector index for the `SensorTempByHour` table:

```sql
/*+primary_key(sensorid, time_hour), index(VECTOR_COSINE, embedding) */
SensorTempByHour := SELECT ... ;
```

### Testing

Add test cases to SQRL scripts with the `/*+test */` hint in front of a table definition:

```sql
/*+test */
InvalidCustomers := SELECT * FROM Customer WHERE name = '' OR email IS NULL ORDER BY customerid;
```

Test annotated tables are only executed when running the [`test` command](compiler#test-command) and otherwise ignored.
DataSQRL queries all test tables at the end of the test and snapshots the results in the [configured](configuration) `snapshot-folder`.

:::warning
Ensure that test tables have a well-defined order and that only predictable columns are selected for the results are stable between test runs.
:::


---
## NEXT_BATCH

```sql
NEXT_BATCH;
```

For SQRL pipelines where the data processing is in batch (i.e. `"execution.runtime-mode": "BATCH"` in the [Flink configuration](configuration-engine/flink)), use the `NEXT_BATCH` statement to break the data processing into multiple sub-batches that are executed sequentially, proceeding with the next sub-batch only if the previous one succeeded.

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


---

## Comments & Doc-strings

* `--` single-line comment
* `/* ... */` multi-line comment
* `/** ... */` **doc-string**: attached to the next definition and propagated to generated API docs.

---

## Validation rules

The following produce compile time errors:

* Duplicate identifiers (tables, functions, relationships).
* Overloaded functions (same name, different arg list) are **not** allowed.
* Argument list problems (missing type, unused arg, unknown type).
* `DISTINCT` must reference existing columns; `ORDER BY` column(s) must be monotonically increasing.
* Basetable inference failure for relationships (see below).
* Invalid or malformed hints (unknown name, wrong delimiter).

---

<!--EXTENDED-->

## Cheat Sheet

| Construct        | Example                                                               |
|------------------|-----------------------------------------------------------------------|
| Import package   | `IMPORT mypackage.sources AS mySources;`                              |
| Internal table   | `CREATE TABLE Orders ( ... );`                                        |
| External table   | `CREATE TABLE kafka_table (...) WITH ('connector'='kafka');`          |
| Table def.       | `BigOrders := SELECT * FROM Orders WHERE amount > 100;`               |
| Distinct         | `Dedup := DISTINCT Events ON id ORDER BY ts DESC;`                    |
| Function         | `OrdersById(id BIGINT) := SELECT * FROM Orders WHERE id = :id;`       |
| Relationship     | `Customer.orders := SELECT * FROM Orders WHERE this.id = customerid;` |
| Column add       | `Orders.total := quantity * price;`                                   |
| Passthrough func | `Func(id BIGINT) RETURNS (col INT) := SELECT raw_sql WHERE id = :id;` |
| Subscription     | `Alerts := SUBSCRIBE SELECT * FROM Dedup WHERE level='WARN';`         |
| Export           | `EXPORT Alerts TO logger.Warnings;`                                   |
| Hint             | `/*+index(hash,id)*/`                                                 |


## More Information

* Refer to the [Configuration documentation](configuration.md) for engine configuration.
* See [Command documentation](compiler.md) for CLI usage of the compiler.
* Read the [How-to guides](/docs/category/-how-to) for best-practices and implementation guidance.
* Follow the [Tutorials](intro/tutorials) for practical SQRL examples.

For engine configuration, see **configuration.md**; for CLI usage, see **compiler.md**.
