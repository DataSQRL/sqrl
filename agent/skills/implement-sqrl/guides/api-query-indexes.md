# Indexes for API Queries

Read this guide only in the cases that implement-sqrl `SKILL.md`, "Indexes for API queries", names.

The compiler selects the indexes for most served queries. Fix only the queries that the case names. Add an `index` hint only to a table that the fix needs it for. Every other table keeps the indexes that the compiler selects.

## Fixing a slow query

1. Find the SQRL definition of each query to fix:
   - A slow-query log names a query by its API field. A top-level path, such as `/OrdersByCustomer`, is the name of the SQRL table or table function. A nested path, such as `/Customers[0]/orders`, names the relationship `orders` of `Customers`. A client alias replaces the field name in the path.
   - The full-table-scan warning names the table and the filter columns that no index serves. Fix the served queries of that table that filter on these columns.
2. Check the indexes of the query (see Checking the indexes).
3. If no index fits a filter, fix the query in this order. Stop at the first step that gives the filter an index:
   1. Rewrite the query into the form in implement-sqrl `SKILL.md`, "Filters in API queries".
   2. For a subquery that cannot become a `JOIN`, follow "Filters inside a subquery".
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
- `JOIN ... ON` conditions, only for the table on the right side of the `JOIN`. The columns of the left table become parameters, and the left table gets no index from the join (see implement-sqrl `SKILL.md`, "Filters in API queries").
- The first `ORDER BY` column, only when the query has no `WHERE` filter and a literal `LIMIT` of 10000 at most. For example, in `WHERE customer_id = :cid ORDER BY amount DESC LIMIT 10`, only `customer_id` gets an index.

## Filters inside a subquery

The compiler selects no index for a filter inside a subquery in a `WHERE` clause, such as `EXISTS (...)` or `IN (SELECT ...)`, and none for the column of the outer table that the subquery compares with. When the subquery cannot become a `JOIN` (see implement-sqrl `SKILL.md`, "Filters in API queries"), keep it and check the compiled plan (see Checking the indexes). For each large table whose filter no index serves, add an `index` hint (see Index hints):

- On the subquery's table: the columns that the subquery compares with a parameter first, then the columns that it compares with the outer query.
- On the outer table: the column that the subquery compares with, but only when the subquery is not part of an `OR`. Inside an `OR`, the database runs the subquery once and reads the whole outer table, with or without that index.

Example: the API returns the orders of all customers who own a given account. `AccountOwners` has the primary key `owner_id`, and a customer can own an account through several of its rows, so a `JOIN` would add duplicate rows.

```sql
-- Subquery: no index for the filter on AccountOwners, and none on Orders.customer_id
OrdersByAccount(accountId STRING) := SELECT * FROM Orders c
WHERE EXISTS (SELECT 1 FROM AccountOwners o
              WHERE o.account_id = :accountId AND o.customer_id = c.customer_id);

-- Hint both tables (Orders only because the subquery is not part of an OR),
-- together with the indexes that other served queries of each table use
/*+index(BTREE, account_id, customer_id) */
AccountOwners := SELECT owner_id, account_id, customer_id FROM ... ;
/*+index(HASH, customer_id) */
Orders := SELECT ... ;
```

## Index hints

Add an `index` hint only to a table that needs it. An `index` hint replaces the automatic index selection for its whole table: the compiler then creates only the hinted indexes and the primary key, and no longer warns about that table. So also add to the hint each index of that table in `deploy/plan/postgres.json` that another served query still uses. Write it as `index(TYPE, column, ...)` hint, not as the `CREATE INDEX` statement. The hint is not limited to the maximum number of indexes that the full-table-scan warning names.

After the compile, check the indexes of the other served queries of that table (see Checking the indexes).

If the table comes from an included project (`script.include`), the `index` hint belongs in that project. If you cannot change that project, report it to its owners.
