# SQRL Language Specification

SQRL is an extension of ANSI SQL —specifically FlinkSQL — that adds language features for **reactive data processing and serving**: table / function / relationship definitions, built-in source & sink management, and an opinionated DAG planner.  
The “R” in **SQRL** stands for *Reactive* and *Relationships*.

Readers are expected to know basic SQL and the [FlinkSQL syntax](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/).  
This document focuses only on features **unique to SQRL**; when SQRL accepts FlinkSQL verbatim we simply refer to the upstream spec.

## Script Structure

A SQRL script is an **ordered list** of statements separated by semicolons (`;`).  
Only one statement is allowed per line, but a statement may span multiple lines.

Typical order of statements:

```text
IMPORT ...        -- import sources from external definitions
CREATE TABLE ...  -- define internal & external sources
other statements (definitions, hints, inserts, exports...)
EXPORT ...        -- explicit sinks
```

At compile time the statements form a *directed-acyclic graph* (DAG).  
Each node is then assigned to an execution engine according to the optimizer and the compiler generates the data processing code for that engine.

## FlinkSQL

SQRL inherits full FlinkSQL grammar for

* `CREATE {TABLE | VIEW | FUNCTION | CATALOG | DATABASE}`
* `SELECT` queries inside any of the above
* `USE ...`

...with the caveat that SQRL currently tracks **Flink 1.19**; later features may not parse.

Refer to the [FlinkSQL documentation](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/) for a detailed specification.

## Type System
In SQRL, every table and function has a type based on how the table represents data. The type determines the semantic validity of queries against tables and how data is processed by different engines.

SQRL assigns one of the following types to tables based on the definition:
- **STREAM**: Represents a stream of immutable records with an assigned timestamp (often referred to as the "event time"). Streams are append-only. Stream tables represent events or actions over time.
- **VERSIONED_STATE**: Contains records with a natural primary key and a timestamp, tracking changes over time to each record, thereby creating a change-stream.
- **STATE**: Similar to VERSIONED_STATE but without tracking the history of changes. Each record is uniquely identified by its natural primary key.
- **LOOKUP**: Supports lookup operations using a primary key but does not allow further processing of the data.
- **STATIC**: Consists of data that does not change over time, such as constants.

## IMPORT Statement

```
IMPORT qualifiedPath (AS identifier)?;
IMPORT qualifiedPath.*;             -- wildcard
IMPORT qualifiedPath.* AS _;        -- hidden wildcard
```

* **Resolution:** the dotted path maps to a relative directory; the final element is the filename stem (e.g. `IMPORT datasqrl.Customer` ⇒ `datasqrl/Customer.table.sql`).
* **Aliases:** rename the imported object (`AS MyTable`).
* **Hidden imports:** prefix the alias with `_` *or* alias a wildcard to `_` to import objects without exposing them in the interface.

Examples:

```sql
IMPORT ecommerceTs.Customer;                 -- visible
IMPORT ecommerceTs.Customer AS _Hidden;      -- hidden
IMPORT ecommerceTs.* AS _;                   -- hide entire package
```

Wild-card imports with aliases *prefix* the alias to all imported table names.


## CREATE TABLE (internal vs external)

SQRL understands the complete FlinkSQL `CREATE TABLE` syntax, but distinguishes between **internal** and **external** source tables. External source tables are standard FlinkSQL tables that connect to an internal data source. Internal tables connect to a data source that is managed by SQRL (depending on the configured `log` engine, e.g. a Kafka topic) and exposed for inserts in the interface.

| Feature                       | Internal source (managed by SQRL)                               | External Source (connector) |
|-------------------------------|-----------------------------------------------------------------|-----------------------------|
| Connector clause `WITH (...)` | **omitted**                                                     | **required**                |
| Computed columns              | Evaluated **on insert**                                         | Delegated to connector      |
| Metadata columns              | `METADATA FROM 'uuid'`, `'timestamp'` are recognised by planner | Passed through              |
| Watermark spec                | Optional                                                        | Passed through              |
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

Must appear **immediately after** the table it extends, and may reference previously added columns of the same table.


## Interfaces

The tables and functions defined in a SQRL script are exposed through an interface. The term "interface" is used generically to describe a means by which a client, user, or external system can access the processed data. The interface depends on the [configured engines](configuration.md#engines-engines): API endpoints for servers, queries and views for databases, and topics for logs. An interface is a sink in the data processing DAG that's defined by a SQRL script.

How a table or function is exposed in the interface depends on the access type. The access type is one of the following:

| Access type         | How to declare                                        | Surface                                     |
|---------------------|-------------------------------------------------------|---------------------------------------------|
| **Query** (default) | no modifier                                           | GraphQL query / SQL view / log topic (pull) |
| **Subscription**    | prefix body with `SUBSCRIBE`                          | GraphQL subscription / push topic           |
| **None**            | object name starts with `_` *or* `/*+no_query*/` hint | hidden                                      |

Example:

```sql
HighTempAlert := SUBSCRIBE
                 SELECT * FROM SensorReading WHERE temperature > 50;
```

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

| Hint                        | Form                                                                       | Applies to     | Effect                                                                                                                                                       |
|-----------------------------|----------------------------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **primary_key**             | `primary_key(col, ...)`                                                    | table          | declare PK when optimiser cannot infer                                                                                                                       |
| **index**                   | `index(type, col, ...)` <br/> Multiple `index(...)` can be comma-separated | table          | override automatic index selection. `type` ∈ `HASH`, `BTREE`, `TEXT`, `VECTOR_COSINE`, `VECTOR_EUCLID`. <br />`index` *alone* disables all automatic indexes |
| **partition_key**           | `partition_key(col, ...)`                                                  | table          | define partition columns for sinks that support partitioning                                                                                                 |
| **vector_dim**              | `vector_dim(col, 1536)`                                                    | table          | declare fixed vector length. This is required when using vector indexes.                                                                                     |
| **query_by_all**            | `query_by_all(col, ...)`                                                   | table          | generate interface with *required* filter arguments                                                                                                          |
| **query_by_any**            | `query_by_any(col, ...)`                                                   | table          | generate interface with *optional* filter arguments                                                                                                          |
| **no_query**                | `no_query`                                                                 | table          | hide from interface                                                                                                                                          |
| **insert**                  | `insert(type)`                                                             | table          | controls the way how mutations will be written to their target sink. `type` ∈ `SINGLE` (default), `BATCH`, `TRANSACTION`                                     |
| **filtered_distinct_order** | flag                                                                       | DISTINCT table | eliminate updates on order column only before dedup                                                                                                          |
| **engine**                  | `enigne(engine_id)`                                                        | table          | pin execution engine (`streams`, `database`, `flink`, ...)                                                                                                   |
| **test**                    | `test`                                                                     | table          | marks test case, only executed with [`test` command](compiler#test-command).                                                                                 |
| **workload**                | `workload`                                                                 | table          | retained as sink for DAG optimization but hidden from interface                                                                                              |

Example:

```sql
/*+primary_key(sensorid, time_hour), index(VECTOR_COSINE, embedding) */
SensorTempByHour := SELECT ... ;
```

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

## Cheat Sheet

| Construct      | Example                                                                |
|----------------|------------------------------------------------------------------------|
| Import package | `IMPORT ecommerceTs.* ;`                                               |
| Hidden import  | `IMPORT ecommerceTs.* AS _ ;`                                          |
| Internal table | `CREATE TABLE Orders ( ... );`                                         |
| External table | `CREATE TABLE kafka_table (...) WITH ('connector'='kafka');`           |
| Table def.     | `BigOrders := SELECT * FROM Orders WHERE amount > 100;`                |
| Distinct       | `Dedup := DISTINCT Events ON id ORDER BY ts DESC;`                     |
| Function       | `OrdersById(id BIGINT) := SELECT * FROM Orders WHERE id = :id;`        |
| Relationship   | `Customer.orders := SELECT * FROM Orders WHERE this.id = customerid;`  |
| Column add     | `Orders.total := quantity * price;`                                    |
| Subscription   | `Alerts := SUBSCRIBE SELECT * FROM Dedup WHERE level='WARN';`          |
| Export         | `EXPORT Alerts TO logger.Warnings;`                                    |
| Hint           | `/*+index(hash,id)*/`                                                  |

## API Mapping

When a server engine is configured, the tables, relationships, and functions defined in a SQRL script map to API endpoints exposed by the server.

### GraphQL

Tables and functions are exposed as query endpoints of the same name and argument signature (i.e. the argument names and types match).
Tables/functions defined with the `SUBSCRIBE` keyword are exposed as subscriptions.
Internal table sources are exposed as mutations with the input type identical to the columns in the table excluding computed columns.

In addition, the result type of the endpoint matches the schema of the table or function. That means, each field of the result type matches a column or relationship on the table/function by name and the field type is compatible.
The field type is compatible with the column/relationship type iff:
* For scalar or collection types there is a native mapping from one type system to the other
* For structured types (i.e. nested or relationship), the mapping applies recursively.

The compiler generates the GraphQL schema automatically from the SQRL script. Add the `--api graphql` flag to the [compile command](compiler.md#compile-command) to write the schema to the `schema.graphqls` file.

You can modify the GraphQL schema and pass it as an additional argument to the compiler to fully control the interface. Any modifications must preserve the mapping described above.

### Base Tables

To avoid generating multiple redundant result types in the API interface, the compiler infers the base table.

The base table for a defined table or function is the right-most table in the relational tree of the SELECT query from the definition body if and only if that table type is equal to the defined table type. If no such table exists, the base table is the table itself.

The result type for a table or function is the result type generated for that table's base table.
Hidden columns, i.e. columns where the name starts with an underscore `_`, are not included in the generated result type.

## More Information

* Refer to the [Configuration documentation](configuration.md) for engine configuration.
* See [Command documentation](compiler.md) for CLI usage of the compiler.
* Read the [How-to guides](howto.md) for best-practices and implementation guidance.
* Follow the [Tutorials](tutorials.md) for practical SQRL examples.

For engine configuration, see **configuration.md**; for CLI usage, see **compiler.md**.
