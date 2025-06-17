# SQRL Language Specification

SQRL is an extension of ANSI SQL, specifically FLinkSQL, which adds support for table, function, and relationship definitions as well as source and sink management. 
The motivation behind SQRL is to extend FlinkSQL into a development language for reactive data processing.
The "R" that SQRL adds to SQL stands for "reactive" and "relationships".

## FlinkSQL

SQRL extends the syntax and semantics of [FlinkSQL](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/) for table and view definitions.

Specifically, SQRL supports the entire FlinkSQL syntax for:

* CREATE TABLE, CATALOG, DATABASE, VIEW, FUNCTION
* SELECT (queries) within the definitions above
* USE

Refer to the [FlinkSQL documentation](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/overview/) for a detailed specification. 
Note, that SQRL may lag behind the most current version of FlinkSQL. SQRL currently extends
FlinkSQL 1.19 - make sure to adjust the version in the documentation links as needed.

The following assumes basic familiarity with ANSI SQL and FlinkSQL in particular, and only specify elements unique to SQRL.

## Statements

SQRL scripts consist of a sequence of statements. Statements are delimited by semi-colon (`;`).
Only one statement is allowed per line but a statement can span multiple lines.

```sql
ValidCustomer := SELECT * FROM Customer WHERE customerid > 0 AND email IS NOT NULL;
```

A statement defines one or multiple tables and functions which can reference previously defined tables/functions. The table and function definitions form a data processing DAG (directed acyclic graph): each table and function definition represents a node in the DAG with edges the tables and functions that occur in its definition body. 

The DAG defines how the data flows from sources to exposed sinks and the data transformations that happen in between. Each node in the DAG is executed by one of the [configured engines](configuration.md#engines).

### Type System
In SQRL, every table and function has a type based on how the table represents data. The type determines the the semantic validity of queries against tables and how data is processed by different engines.

SQRL assigns one of the following types to tables based on the definition:
- **STREAM**: Represents a stream of immutable records with an assigned timestamp (often referred to as the "event time"). Stream tables represent events or actions over time.
- **VERSIONED_STATE**: Contains records with a natural primary key and a timestamp, tracking changes over time to each record, thereby creating a change-stream.
- **STATE**: Similar to VERSIONED_STATE but without tracking the history of changes. Each record is uniquely identified by its natural primary key.
- **LOOKUP**: Supports lookup operations using a primary key but does not allow further processing of the data.
- **STATIC**: Consists of data that does not change over time, such as constants.

## IMPORT Statement

*IMPORT qualifiedName (AS? identifier)?*

An import in SQRL describes a **table**, **function**, or other **sqrl script** to be added to the schema. Import paths use the dot character `.` to separate path components.

```sql
IMPORT datasqrl.seedshop.Orders;
```

The last element of the import path is the table or function identifier. The prior elements define the filesystem paths relative to the build directory where the table or function definition can be found. The example above maps to the relative path `datasqrl/seedshop/` for the identifier `Orders` which resolves to the filename `Orders.table.sql` that contains the table definition. Take a look at [the advanced configuration](howto) for more information on importing functions and other scripts.

Paths are case insensitive. Import paths can end with a `*` to import all items on that level of the qualified path.
```sql
IMPORT mypackage.*;
```

Imports can be aliased using the `AS` keyword. Imports that end with a `*` cannot be aliased.
```sql
IMPORT datasqrl.seedshop.Orders AS MyOrders;
```

## CREATE TABLE Statement

```sql
CREATE TABLE Customer (
  customerid BIGINT,
  email STRING,
  update_time TIMESTAMP_LTZ(3)
) WITH (
    'connector': 'datagen'
);
```
CREATE TABLE statements define a data source in SQRL. SQRL supports the full FlinkSQL syntax for creating tables with connectors to external data sources.

A table created without a connector is an "internal" data source that is managed by compiled data pipeline. A table with a connector is managed by an external data system that the compiled data pipeline connects to and are therefore "external" data sources. External data sources with connectors are treated exactly as they are in Flink.

### CREATE TABLE Interfaces

Tables created in SQRL without a connector are exposed through an [interface](#interface) that data can be inserted into. The following special conventions apply to these tables:

* Only unenforced primary keys are supported which means SQRL will not fail an insert if a record for the primary key already exists but instead update the record. In other words, all inserts are upserts.
* The `timestamp` metadata column represents the insert time of a record.
* All computed columns (i.e. columns that are defined by an expression) in the table schema definition are computed prior to or upon insert into the table, which means the data is static when being read. This applies specifically to non-deterministic and time functions.
* If the table contains a column named `_uuid` that column is treated as a computed column with the field value computed on insert as a randomly generated UUID. If no primary key is defined, this column is considered to uniquely identify a record in the data stream represented by the table.

```sql
CREATE TABLE Customer (
  customerid BIGINT,
  email STRING,
  update_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  PRIMARY KEY (customerid) NOT ENFORCED
);
```

## Definition Statement

In addition to the CREATE VIEW statement that FlinkSQL supports, SQRL recognizes the following statements for defining tables and functions.

SQRL native definitions use the assignment operator `:=` to define the element on the left-hand side with the definition body on the right-hand side.

Definitions are uniquely identified by name and trying to reuse a name will produce an error. This also applies to functions with different signatures as SQRL does not support overloading.

### Table Definition Statement

*identifier := select-query;*

Defines a new table as a select query. This operation translates to a `CREATE VIEW` statement in conventional SQL syntax.

```sql
ValidCustomer := SELECT * FROM Customer WHERE customerid > 0 AND email IS NOT NULL;
```

### DISTINCT Statement

*identifier := DISTINCT identifier ON column-names ORDER BY column-name;*

Distinct statements select the most recent version of each row (determined by the `ORDER BY` clause) for each primary key (determined by the `ON` columns). 
The distinct operator deduplicates a stream table that represents a changelog stream into the underlying state table. 

```sql
DistinctProducts := DISTINCT Products ON id ORDER BY updated DESC;
```

### Function Definition Statement

*identifier(arguments-declaration) := select-query;*

Defines a new function with the declared list of arguments which constitute the function signature. The body of a function definition is a SELECT query that references the arguments as variables. An argument is referenced by name preceded by a column `:`.

```sql
CustomerByEmail(email STRING) := SELECT * FROM Customer WHERE email = :email;
```

### Relationship Definition Statement

*parent-table.identifier(arguments-declaration) := select-query;* 

A relationship is a function defined within the context of a parent table.
A relationship relates records from the parent table to other records through
the SELECT query in the definition body.

Use `this.column-name` to reference column from the parent table in the SELECT query. For relationships, arguments are optional.

```sql
Customer.orders := SELECT * FROM Orders o WHERE o.customerid = this.id;
```
Defines a relationship without arguments. It references the `id` column from the parent `Customer` table via `this.id`.

```sql
Customer.highValueOrders(minAmount BIGINT) := SELECT * FROM Orders o WHERE o.customerid = this.id AND o.amount > :minAmount;
```
This relationship is similar to the one defined above but with an argument `minAmount` that is referenced in the SELECT query as the variable `:minAmount`.

### Column Addition Statement

*table-name.column-name := expression*

Adds a column to a previously defined table defined via a SQL expression that's in the body of the definition.
Columns can only be added immediately following the table definition. Multiple columns can be added to the same table.

Adding columns is useful for expressions that reference other columns in the table.

```sql
OrderEntries := SELECT o.id, o.time, e.productid, e.q AS quantity, e.p AS unit_price, coalesce(e.d,0.0) AS discount FROM Orders o CROSS JOIN UNNEST(o.entries) e;
OrderEntries.total = quantity * unit_price - discount;
```

## Interface

The tables and functions defined in a SQRL script are exposed through an interface. The term "interface" is used generically to describe a means by which a client, user, or external system can access the processed data. The interface depends on the [configured engines](configuration.md#engines): API endpoints for servers, queries and views for databases, and topics for logs. An interface is a sink in the data processing DAG that's defined by a SQRL script.

How a table or function is exposed in the interface depends on the access type. The access type is one of the following:

* **Query**: The data is returned on request, e.g. by querying the database, accessing a GraphQL query endpoint on the server, or accessing a view.
* **Subscription**: The data is pushed to the client, e.g. by publishing it to a topic in the log, a GraphQL subscription on the server, etc.
* **None**: The data is not exposed in the interface.

The default access type is "Query".

### Hidden Tables and Functions

Hidden tables and functions have access type "None". A table or function is hidden if the name starts with an underscore `_`.

```sql
_HiddenCustomer := SELECT id, email FROM Customer;
```

### SUBSCRIBE Statement

Using the `SUBSCRIBE` keyword in front of the SELECT query in the definition body sets the access type for a table or function to "Subscription".

```sql
HighOrderAlert := SUBSCRIBE SELECT * FROM Orders WHERE total > 1000;
```

### CREATE TABLE

CREATE TABLE statements that define an [internal data source](#create-table-interfaces) are exposed as topics in the log, or GraphQL mutations in the server.
The input type is defined by mapping all column types to native data types of the interface schema. Computed and metadata columns are not included in the input type since those are computed on insert.

### API Types

When the interface is an API, tables and functions are exposed through endpoints of the same name and argument signature (i.e. the argument names and types must match).

In addition, the result type of the endpoint must match the schema of the table or function. That means, each field of the result type must match a column of or relationship on the table/function by name and the field type must be compatible.
The field type is compatible with the column/relationship type iff:
* For scalar or collection types there is a native mapping from one type system to the other
* For structured types (i.e. nested or relationship), the mapping applies recursively.

#### Base Tables

In the case where the API types are generated from the SQRL script the collection of result types is reduced by only generating result types for base tables identified by the base table name.

The base table for a defined table or function is the right-most table in the relational tree of the SELECT query from the definition body if and only if that table type is equal to the defined table type. If no such table exists, the base table is the table itself.

The result type for a table or function is the result type generated for that table's base table.
Hidden columns, i.e. columns where the name starts with an underscore `_`, are not included in the generated result type.

### EXPORT Statement

*EXPORT identifier TO qualifiedName;*

`EXPORT` statements define an explicit subscription interface to an external data system. The `EXPORT` statement is an explicit sink to a data system, like a kafka topic or database table. Import paths and export sink paths are resolved the same way with the last element of the path being the identifier and the prior elements defining the relative filesystem path.

Sinks are defined using FlinkSQL's CREATE TABLE syntax with connector configuration.

```sql
EXPORT UserPromotion TO mysink.promotion;
```

SQRL supports a few generic sinks that do not have to be defined explicitly:

* **print**: Prints table records to stdout with the identifier as a prefix.
* **logger**: Sends the table records to the [configured](configuration.md#compiler) logger.
* **log**: Publishes the table records to a topic in the [configured log engine](configuration.md#engines). Creates a topic with the identifier as the name.

```sql
EXPORT UserPromotion TO logger.promotion;
```

## Comments

SQRL supports the use of comments within the code to provide hints, enhance readability, provide documentation, and explain the logic of complex queries or operations.

SQRL supports two types of comments:

**Single-line Comments**: These are initiated with double dashes `--`. Everything following the `--` on the same line is considered part of the comment.
```sql
-- This is a single-line comment explaining the next SQL command
NewTable := SELECT * FROM OtherTable;
```
Only single-line comments are supported at the end of statements on the same line.

**Multi-line Comments**: These are enclosed between `/*` and `*/`. Everything within these markers is treated as a comment, regardless of the number of lines.
```sql
/*
 * This is a multi-line comment.
 * It can span multiple lines and is often used to comment out
 * chunks of code or to provide detailed documentation.
 */
NewTable := SELECT * FROM OtherTable;
```

**Documentation**
```sql
/**
 This is a doc-string that describes the table or function.
 */
CustomerByEmail(email STRING) := SELECT * FROM Customer WHERE email = :email;
```

Multi-line comments that start with `/**` are documentations strings. Documentation strings are added to the API documentation (if a server engine is configured) and generated metadata for a table or function.

## SQRL Hints

SQRL Hints are included within multi-line comments and are prefixed with a plus sign `+` followed by the hint type and optional parameters. These hints do not alter the SQL semantics (like FlinkSQL hints inside SELECT queries do that SQRL also supports) but are used by the SQRL planner and optimizer. 

Hints are placed *before* assignment statements.

```sql
-- The below hint suggests that the following query should be executed in Postgres.
/* +exec(postgres) */
MyTable := SELECT * FROM InventoryStream;
```

SQRL supports the following hints.

### Primary Key Hint

*+primary_key(column-names...)*

Assigns a primary key to the table defined below. The primary key uniquely identifies records in the table. This is needed when SQRL cannot automatically determine the primary key and the table is persisted to a database that requires one.

```sql
/*+primary_key(id, productid) */
OrderEntries := SELECT o.id, o.time, e.productid, e.quantity FROM Orders o CROSS JOIN UNNEST(o.entries) e;
```

This hint only applies to table definitions.

### Query Hints

*+query_by_all(column-names...)*

*+query_by_any(column-names...)*

*+no_query*

By default, the query or subscription interface for a table returns all records in that table without filters. Query hints modify the interface:

* **query_by_all**: The interface for the table filters by the columns listed in the hint. That means, the interface has an argument for each column and filters for the records where the provided values match the column values (i.e. an AND condition of `column = column-value`). Values must be provided for all columns (i.e. none are optional).
* **query_by_any**: The interface for the table filters by the columns listed in the hint like `query_by_all` but values for the columns are optional. If a value is not provided for a column the filter does not apply for that column (i.e. an AND condition of `column-value IS NULL OR column = column-value`).
* **no_query**: Sets the access type for the table to "None" so that no interface is generated.

```sql
/*+query_by_any(email, name) */
Customers := SELECT id, email, name, last_updated FROM CustomerStream WHERE id IS NOT NULL; 
```

This hint only applies to table definitions.

### Test Hint

*+test*

Marks a table as a test case. Test tables are only exposed when executing tests, otherwise their access type is "None". See [the test runner documentation](compiler#test-command) for more details on testing SQRL scripts.

```sql
/*+test */
CustomerTest := SELECT * FROM Customer WHERE email IS NULL ORDER BY id ASC; 
```

This hint only applies to table definitions.

### Workload Hint

*+workload*

Marks a table as a workload which means that the access type is "None" but the table is included as a sink in the data processing DAG and therefore included in the planning and optimization.

Tables that simulate user workloads are annotated with this hint. This hint is often combined with `+test` to include test cases in the planning without exposing them in the interface.

```sql
/*+test, workload */
CustomerByIdTest := SELECT * FROM Customer WHERE id = 5;
```

This hint only applies to table definitions.

### Execution Hint

*+exec(engine_name)*

An execution hint specifically defines the engine that executes this table. Usually, the DataSQRL optimizer will determine which engine executes a particular table based on a cost model. This hint gives the user control over that assignment.
The optimizer will optimize the other assignments based on the user provided ones. Beware that tables which are downstream from other tables must be assigned engines that sit downstream in the infrastructure topology. Otherwise DataSQRL cannot find a feasible computation DAG.

### Index Hint

*+index(index-type, column-names...)*

For tables that are persisted into a database, the SQRL optimizer automatically determines index structures based on the exposed interface and workload queries. With the index hint, index structures can be defined manually and overwrite the optimized indexes.

An index hint takes the type as the first argument and a list of columns to index in the given order.
The following index types are supported:

* **HASH**: A hash index
* **BTREE**: A b-tree index
* **TEXT**: A text search index
* **VECTOR_COSINE**: A vector index using cosine distance
* **VECTOR_EUCLID**: A vector index using euclid or L2 distance

If an index hint with no argument is provided, no index structures will be created for that table.

```sql
/*+index(btree, customerid, last_updated) */
Customers := SELECT id, email, name, last_updated FROM CustomerStream; 
```

This hint only applies to table definitions.

### Partition Key Hint

*+partition_key(column-names...)*

For tables that are persisted into an external data system that supports data partitioning, the `partition_key` hint defines the list of columns by which the data is partitioned.

```sql
/*+partition_key(partition_id) */
Customers := SELECT id % 8 AS partition_id, id, email, name, last_updated FROM CustomerStream; 
```

This hint only applies to table definitions.