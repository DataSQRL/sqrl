# Indexes for API Queries

Read this guide only in these cases:

- The requirements, the plan or a review issue name a slow query or an index.
- A served query reads a large table, and you find that no index from the compiler serves its filter.

The compiler selects the indexes for most served queries. Fix only the slow query. Add an `index` hint only to a table that the fix needs it for. Every other table keeps the indexes that the compiler selects.

## Fixing a slow query

1. Find the SQRL definition of the slow query. A slow-query log names a query by its API field. A top-level path, such as `/OrdersByCustomer`, is the name of the SQRL table or table function. A nested path, such as `/Customers[0]/orders`, names the relationship `orders` of `Customers`. A client alias replaces the field name in the path.
2. Check the indexes of the query (see Checking the indexes).
3. If no index fits a filter, fix the query in this order. Stop at the first step that gives the filter an index:
   1. Rewrite the filter into the form in implement-sqrl `SKILL.md`, "Filters in API queries". In a `JOIN`, list the table that the `WHERE` clause filters with a parameter first (see Automatic index selection).
   2. For a filter inside a subquery, follow "Filters inside a subquery".
   3. Add an `index` hint to the table that the filter reads (see Index hints).
   4. When no index can serve the filter, denormalize: compute the value in Flink and store it as a column of the served table.
4. Compile again and repeat the steps in "Checking the indexes". Make sure that an index now fits the filter.
5. Run the tests.

## Checking the indexes

Judge the table size from the requirements and the source data. For the query that you fix:

1. Find the SQL of the query in `deploy/plan/vertx.json`, and the `CREATE INDEX` statements of the table in `deploy/plan/postgres.json`. The primary key also counts as an index.
2. Compare the columns that the query compares with a parameter or with another table, in the `WHERE` clause, in `JOIN ... ON` conditions and in subqueries, with the leading columns of the indexes. In a correlated subquery that is not part of an `OR`, the column of the outer table counts too: in the `EXISTS` example below, `Orders.customer_id` gets no index.
3. Make sure that each of these columns stands alone in the SQL. The compiler also creates an index for `day_index + 1 = $1`, but the database cannot use it for that filter.

## Automatic index selection

The compiler selects the indexes from these parts of the served queries:

- The filters of `WHERE` clauses, in table functions, relationships and `query_by_*` arguments.
- Table order in the `FROM` clause matters for `JOIN ... ON` conditions: only the table on the right side of the `JOIN` gets an index from the join condition. The columns of the left table become parameters, and the left table gets no index from the join. List the table that the `WHERE` clause filters with a parameter first:
  - `FROM Orders c JOIN AccountOwners o ON o.customer_id = c.customer_id WHERE o.account_id = :accountId` gives an index only to `AccountOwners` (`account_id, customer_id`), and the database reads all of `Orders`.
  - `FROM AccountOwners o JOIN Orders c ON c.customer_id = o.customer_id WHERE o.account_id = :accountId` gives an index to both tables: `AccountOwners` (`account_id`) and `Orders` (`customer_id`).
- The first `ORDER BY` column, only when the query has no `WHERE` filter and a literal `LIMIT` of 10000 at most. For example, in `WHERE customer_id = :cid ORDER BY amount DESC LIMIT 10`, only `customer_id` gets an index.

## Filters inside a subquery

The compiler selects no index for a filter inside a subquery in a `WHERE` clause, such as `EXISTS (...)` or `IN (SELECT ...)`, and none for the column of the outer table that the subquery compares with. Check the compiled plan (see Checking the indexes). If the subquery's table is large and has no index for the subquery's filter, handle the filter in this order:

1. Rewrite the subquery as a `JOIN`, with the subquery's table first. Do this only when the join adds no duplicate rows: after the subquery's filter, its table has at most one matching row for each row of the outer table, for example because its primary key consists of the filter and join columns.
2. When a join cannot replace the subquery, because it would add duplicate rows or the subquery is part of an `OR`, keep the subquery and add an `index` hint to the subquery's table: the columns that the subquery compares with a parameter first, then the columns that it compares with the outer query. Also hint the outer table's column that the subquery compares with, but only when the subquery is not part of an `OR`. Inside an `OR`, the database runs the subquery once and reads the whole outer table, with or without that index.

Example: the API returns the orders of all customers who own a given account.

```sql
-- Subquery: no index for the filter on AccountOwners, and none on Orders.customer_id
OrdersByAccount(accountId STRING) := SELECT * FROM Orders c
WHERE EXISTS (SELECT 1 FROM AccountOwners o
              WHERE o.account_id = :accountId AND o.customer_id = c.customer_id);

-- Step 1, join: the primary key (account_id, customer_id) serves AccountOwners, and Orders gets an index on customer_id
OrdersByAccount(accountId STRING) := SELECT c.* FROM AccountOwners o JOIN Orders c ON c.customer_id = o.customer_id
WHERE o.account_id = :accountId;

-- Step 2, when the join cannot be used: hint the subquery's table, and the outer table only if the subquery is not part of an OR
/*+index(BTREE, account_id, customer_id) */
AccountOwners := SELECT account_id, customer_id FROM ... ;
/*+index(HASH, customer_id) */
Orders := SELECT ... ;
```

## Index hints

Add an `index` hint only to a table that needs it. An `index` hint replaces the automatic index selection for its whole table: the compiler then creates only the hinted indexes and the primary key. So also add to the hint each index of that table in `deploy/plan/postgres.json` that another served query still uses. Write it as `index(TYPE, column, ...)` hint, not as the `CREATE INDEX` statement.

After the compile, check the indexes of the other served queries of that table (see Checking the indexes).

If the table comes from an included project (`script.include`), the `index` hint belongs in that project. If you cannot change that project, report it to its owners.
