import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# SQRL Specification
This is the specification for SQRL, a declarative SQL query language developed at DataSQRL for describing data pipelines. SQRL stands for *"**S**tructured **Q**uery and **R**eaction **L**anguage"* because it extends SQL with support for streaming data and the ability to react to data in realtime. In addition, SQRL adds a number of convenience features that make it development-friendly.

## Table Type System
In SQRL, every table is assigned a specific type that influences how queries interact with the data, the semantic validity of those queries, and how data is processed by different engines.

SQRL recognizes several distinct table types, each with unique characteristics and use cases:
- **STREAM**: Comprises a stream of immutable records, each identified by a synthetic primary key and timestamp. These tables are ideal for representing events or actions over time.
- **VERSIONED_STATE**: Contains records with a natural primary key and a timestamp, tracking changes over time to each record, thereby creating a change-stream.
- **STATE**: Similar to VERSIONED_STATE but without tracking the history of changes. Each record is uniquely identified by its natural primary key.
- **LOOKUP**: Supports lookup operations using a primary key but does not allow further processing of the data.
- **RELATION**: Represents relational data that lacks a primary key, timestamp, or explicit streaming semantics. It is used primarily for static relational data integration.
- **STATIC**: Consists of data that does not change over time, such as constants, table functions, or nested data structures. This type is treated as universally valid across all time points.

These table types will be used throughout this specification to further describe the semantics of sql queries.

## Functions
Functions in SQRL are designed to be engine-agnostic, ensuring that their implementation is consistent across different platforms and execution environments. This uniformity is crucial for maintaining the semantic integrity of functions when executed under various systems.

**Characteristics of Functions**
- **Engine Agnosticism**: Functions are defined in a way that does not depend on the specifics of the underlying engine.
- **Semantic Consistency**: Regardless of the engine used, function should preserve their semantic meaning.
- **Mixed Engine Support**: While functions are designed to be widely supported, some may have mixed support depending on the engine's capabilities.
- **Nullability Awareness**: Functions in SQRL retain nullability information. This feature is vital for correct schema generation downstream, ensuring that data integrity is maintained through the potential propagation of null values.
- **Time-Preserving Capabilities**: A significant feature of SQRL functions is their ability to handle time-based data efficiently. Time-preserving functions can manipulate and interpret timestamps in a way that aligns with the temporal dynamics of data streams.

For example, a time-preserving function called 'endOfWeek' could be implemented to aggregate timestamps into time windows. Time windows are a means to divide time into discrete buckets and aggregate all stream records within each bucket to produce a new stream table that contains one row for each aggregate.
```sql
Users.spending := SELECT endOfWeek(p.time) AS week,
         sum(t.price) AS spend, sum(t.saving) AS saved
      FROM @.purchases p JOIN p.totals t
      GROUP BY week ORDER BY week DESC;
```

## Import
*IMPORT qualifiedName (AS? alias=identifier)?*

An import in SQRL describes a **table**, **function**, or other **sqrl script** to be added to the schema. Import paths use the dot character `.` to separate path components.

```sql
IMPORT datasqrl.seedshop.Orders;
```
Imports are intended to act much like language dependencies. The SQRL specification does not describe how imports are resolved and is up to the implementation.

Imports can end with a `*` to import all items on that level of the qualified path.
```sql
IMPORT mypackage.*;
```

Imports can be aliased using the `AS` keyword. Imports that end with a `*` cannot be aliased.
```sql
IMPORT datasqrl.seedshop.Orders AS MyOrders;
```

## Export

*EXPORT table=tablePath TO sink=qualifiedName;*

The `EXPORT` statement is an explicit sink to a data system, like a kafka topic or database table. Import paths and export sink paths should be resolved the same way.

```sql
EXPORT UserPromotion TO mysink.promotion;
```

Export statements are most commonly used to export data to an external system, but it could refer to other components such as console log print statements or to a log engine. It is up to the underlying implementation to determine what modules are available by default for import and export paths. 
```sql
EXPORT UserPromotion TO print.promotion;
EXPORT UserPromotion TO log.promotion;
```

:::note
Exports do not describe the connector mapping when an optimizer splits the workload between multiple engines. 
:::

## Create Table
```sql
CREATE TABLE MyTable(
  myCol bigint,
  myCol2 bigint
);
```
SQRL allows a 'create table' statement. Create table statements describe a table that the SQRL implementation provides storage for. The implementation can optionally bring additional fields, such as a primary key and timestamp.

## Assignment operator
The assignment operator `:=` is used to specify the structure and contents of the right-hand side to the left-hand side. This operation is akin to the 'CREATE VIEW' statement in conventional SQL.

## Distinct
```sql
Products := DISTINCT Products ON id ORDER BY updated DESC;
```

Distinct statements in SQRL are designed to select the most recent version of each row based on a specified key, effectively implementing deduplication in streaming data or ensuring data uniqueness in database systems.

## Queries
```sql
MyTable := SELECT * FROM Table;
```

**Naming and Selecting Columns**

In SQRL, all columns must be explicitly named.
```sql
// Invalid
MyTable := SELECT COUNT() FROM Table;

// Valid
MyTable := SELECT COUNT() AS cnt FROM Table;
```

**Table Shadowing**

Tables can be shadowed, meaning a new table can be created with the same name as an existing one.

```sql
MyTable := SELECT * FROM MyTable;
```

**Hidden Tables and Columns**

When the name of a table or column starts with the underscore character _, it is considered hidden. Hidden tables and columns are not exposed in the API or imported by other scripts.

```sql
_MyHiddenTable := SELECT * FROM MyTable WHERE ...;
```

Example of Hidden Column:

```
MyTable := SELECT id, _hiddenColumn FROM Table;
```
In this example, _hiddenColumn will not be exposed in the API.

**Usage of Hidden Tables**

Hidden tables are useful for intermediate calculations or data transformations that should not be accessible externally.

```sql
_tempData := SELECT * FROM MyTable WHERE condition;
```
In this example, _tempData is used for an internal operation and is not exposed.

## Nested Query
Nested tables represent parent-child relationships and simplify aggregations by parent rows.
```
MyTable.query := SELECT * FROM x;
```

We can query a nested table globally, i.e., over all rows in the table, or locally, i.e., only the rows associated with a given parent row.

**Global Aggregation**
```sql
Order_totals := SELECT sum(total) as price, 
    sum(coalesce(discount, 0.0)) as saving FROM Orders.items;
```
In this example, the Order_totals table contains a single aggregate that sums up the total and discount over all items in all orders. The result is one global aggregation over all order items.

**Local Aggregation**
```sql
Orders.totals := SELECT sum(total) as price, 
    sum(coalesce(discount, 0.0)) as saving FROM @.items;
```
This statement aggregates all items for each order. The result is one local aggregate for each row in the Orders table.

**Difference Between Global and Local Aggregation**

The difference between the two statements lies in the FROM clause. The first statement references the Orders.items table globally, meaning it considers all rows in the Orders.items table without any specific parent context.

The second statement references the Orders.items table locally by accessing the items relationship column on Orders. This makes it a localized query, defining a new nested table totals under the Orders table. The query on the right-hand side of the statement is interpreted in the context of each row in the parent table. The at-sign @ is used to refer to the parent row in a localized query. Hence, @.items means "all items that are associated with the current order record through the items relationship".

**Usage of Nested Table Definitions**

Nested table definitions are a convenient way to express GROUP BY and WINDOW queries by grouping on the rows in the parent table. This allows for more intuitive and organized data aggregation, making it easier to manage complex data relationships and calculations.

#### Join types
SQRL provides additional join types outside of standard SQL:
- Default join
- Temporal join
- Interval join

**Default Join**

A join without a qualifier is a default join. It is up to the implementation to decide the best join type given the conditions of the join.
```sql
DefaultJoinExample := SELECT * FROM TableA JOIN TableB ON TableA.id = TableB.id;
```

**Inner Join**

Inner joins are explicit inner joins. In stream processing contexts, this can mean maintaining the state on both sides of the join to allow proper semantics, which can be expensive.
```sql
InnerJoinExample := SELECT * FROM TableA INNER JOIN TableB ON TableA.id = TableB.id;
```

**Left Join and Left Outer Join**

In SQRL, a left join and a left outer join are distinct. Left joins can let the implementation decide the best join type. Left outer joins are explicit left joins.

```sql
LeftJoinExample := SELECT * FROM TableA LEFT JOIN TableB ON TableA.id = TableB.id;
LeftOuterJoinExample := SELECT * FROM TableA LEFT OUTER JOIN TableB ON TableA.id = TableB.id;
```

**Temporal Join**

SQRL supports temporal joins between stream and state tables when joining on the state table's key. Temporal joins use the row from the state table at the timestamp of the stream row.

```sql
TemporalJoinExample := 
  SELECT l.login_time, t.transaction_time, t.amount
  FROM Logins l 
  TEMPORAL JOIN Transactions t 
  ON l.account_id = t.account_id;
```

**Interval Join**

Interval joins are defined by specifying upper and lower time bounds. The INTERVAL JOIN condition specifies that a transaction must occur within a specified time frame after a login to be included in the join.
```sql
CustomerActivity := 
  SELECT l.login_time, t.transaction_time, t.amount
  FROM Logins l 
  INTERVAL JOIN Transactions t 
  ON l.account_id = t.account_id 
  AND t.transaction_time BETWEEN l.login_time AND l.login_time + INTERVAL '1' HOUR;
```
This correlates logins with transactions that happen shortly after, capturing a critical timeframe for activity analysis.

**Example: Temporal Joins in E-commerce**

Temporal joins are essential in SQRL as they define different semantics, joining stream tables with state tables. For example, in an e-commerce scenario, if the price changes on a product, you do not want to retroactively update already placed orders.
```sql
IMPORT ecommerce.Orders;  // is a stream
IMPORT ecommerce.Products;  // is a stream

VersionedProducts := DISTINCT Product ON productid;  // converts to a versioned state table

OrdersWithPrice := 
  SELECT * 
  FROM Orders
  JOIN VersionedProducts;  // join on stream and state becomes a temporal join
```
In this example, VersionedProducts becomes a versioned state table, and the join with Orders ensures that each order reflects the product price at the time of the order, not the current product price.

## Expressions
Expressions in SQRL allow you to define new columns based on calculations or transformations of existing columns.
```sql
Products.weight_in_oz := weight_in_gram / 28.35;
```

**Defining New Columns**

This statement adds a new column weight_in_oz to the existing Products table, which converts the product weight to ounces. The column name is the last item in the table path.

**Expression Constraints**

Expressions cannot be shadowed. Once an expression is defined, it cannot be overridden or redefined in the same context.

Once a table is queried, new columns cannot be added. Tables become immutable once referenced in a query.

**Nested Expressions and Window Functions**

Nested expressions can evaluate window functions, allowing for calculations over a set of rows related to the current row.

```sql
MyRow.num := RANK();
```
In this example, MyRow.num is defined using the RANK() window function, which assigns a rank to each row within the partition of the dataset.

## Relationships
Relationships in SQRL make data relationships explicit, simplify joins, and allow API consumers to navigate through the data efficiently.

**Defining Relationships**

A relationship is defined using a JOIN expression, interpreted for each row of the table on which the relationship is defined. The at-sign @ refers to each row.

```sql
Users.purchases := JOIN Orders ON Orders.customerid = @.id;
```
This statement defines a purchases column in the Users table, relating each user to their corresponding orders where customerid matches the user's id.

**Benefits**
- **Simplifies Joins**: Avoids repetitive join statements.
- **Explicit Relationships**: Makes data relationships clear and easy to follow

**Using Relationships in Queries**
```sql
Users.spending := 
  SELECT endOfWeek(p.time) AS week,
         sum(t.price) AS spend, 
         sum(t.saving) AS saved
  FROM @.purchases p 
  JOIN p.totals t
  GROUP BY week 
  ORDER BY week DESC;
```
This example defines a spending nested table under Users, aggregating order totals for all purchases of each user. The FROM @.purchases expands to FROM @ JOIN Orders p ON p.customerid = @.id.

## Parameters
Join declarations and tables in SQRL support parameters, allowing for dynamic queries.

Parameters use the SQL variable syntax, the `@` followed by the variable name. This is not to be confused with localized queries

```sql
MyTable.byId(@val: BIGINT) := JOIN Table t ON t.id = @.id AND @val > 50;
MyTableById(@id: STRING) := SELECT * FROM Table t WHERE t.id = @id;
```

**Arguments**

Arguments may be provided in any syntactic order and maintain identical semantic meaning.
<!--
Parameters are
1. Ordered
2. Named

A named parameter can be referenced:
```sql
MyTable := SELECT * FROM table1.table2(name => x).table3;
```
-->

Parameterized queries are useful when describing different views of a table.
```sql
ProductById(@ProductID: String) := 
  SELECT * FROM UniqueInventory 
  WHERE ProductID = @ProductID;
```

## Table Paths
In SQRL, table paths are traversed like a graph, not as subschemas.

```sql
Users.spending := SELECT endOfWeek(p.time) AS week,
         sum(t.price) AS spend, sum(t.saving) AS saved
      FROM @.purchases p JOIN p.totals t
      GROUP BY week ORDER BY week DESC;
```
This statement defines a nested table `spending` underneath `Users` which aggregates over the nested order `totals` for all purchases of each user. Relationships used in `FROM` and `JOIN` are expanded to their original definition. That means, `FROM @.purchases` gets expanded to `FROM @ JOIN Orders p ON p.customerid = @.id`.


## Comments
SQRL supports the use of comments within the code to provide hints, enhance readability, provide documentation, and explain the logic of complex queries or operations.

SQRL supports two types of comments:

**Single-line Comments**: These are initiated with double forward slashes (//). Everything following the // on the same line is considered part of the comment.
```sql
// This is a single-line comment explaining the next SQL command
IMPORT data.SalesRecords;
```

**Multi-line Comments**: These are enclosed between /* and */. Everything within these markers is treated as a comment, regardless of the number of lines. T
```sql
/*
 * This is a multi-line comment.
 * It can span multiple lines and is often used to comment out
 * chunks of code or to provide detailed documentation.
 */
IMPORT data.StockAdjustments;
```

## Hints
Hints are included within multi-line comments and are prefixed with a plus sign (+) followed by the hint type and optional parameters. These hints do not alter the SQL syntax but suggest how the underlying engine should treat the subsequent SQL commands. Hints are placed above assignment statements.

```sql
// The below hint suggests that the following query should be executed on the stream engine.
/* +exec(streams) */
MyTable := SELECT * FROM InventoryStream;
```
