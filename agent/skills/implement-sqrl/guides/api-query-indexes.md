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
   2. For a filter on another table, use a subquery form that the compiler indexes (see "Filters inside a subquery").
   3. Add an `index` hint to the table that the filter reads (see Index hints).
   4. When no index can serve the filter, compute the value in a table with `/*+engine(flink) */`, so that Postgres stores it as a column (see "Where a table runs" in [SKILL.md](../SKILL.md#where-a-table-runs)).
4. Compile again and repeat the steps in "Checking the indexes". Make sure that an index now fits the filter.
5. Run the tests.

## Checking the indexes

Judge the table size from the requirements and the source data. For the query that you fix:

1. Find the SQL of the query in `deploy/plan/vertx.json`, and the indexes of the table in `pipeline_explain.txt`, in the `Table` section after the `--- physical ---` line of the table. The section uses the table and index names of the SQL, for example the line `index Orders_2_btree_c1c2 (btree): customerid, time` under `Table Orders_2 (postgres):`. The primary key also counts as an index. A table that runs in Postgres has no indexes of its own and no `Table` section. Its query is part of the SQL of the served query, and the compiler creates its indexes on the stored tables that this query reads.
2. Compare the columns that the query compares with a parameter or with another table, in the `WHERE` clause, in `JOIN ... ON` conditions and in subqueries, with the leading columns of the indexes. In an `EXISTS (...)` or `IN (SELECT ...)` that is not part of an `OR`, the column of the outer table that the subquery compares with counts too.
3. Make sure that each of these columns stands alone in the SQL. For a filter that wraps its column in an expression, such as `day_index + 1 = :day`, the compiler creates no index and warns `The filter ... cannot use an index on table ...`. When the filtered column comes from a table that runs in Postgres, the warning shows the expression of that table's query, such as `CAST(event_time AS STRING) = :t` for its column `t_str`.

## Automatic index selection

The compiler selects the indexes from these parts of the served queries:

- The filters of `WHERE` clauses, in table functions, relationships and `query_by_*` arguments.
- `JOIN ... ON` conditions, only for the table on the right side of the `JOIN`. The columns of the left table become parameters, and the left table gets no index from the join (see implement-sqrl `SKILL.md`, "Filters in API queries").
- Subqueries, such as `EXISTS (...)`, `IN (SELECT ...)` or `(SELECT COUNT(*) ...)` (see Filters inside a subquery).
- `text_search(...) > 0` filters: a `TEXT` index (GIN in Postgres) on the searched columns (see implement-sqrl `functions/text.md`).
- The first `ORDER BY` column, only when the query has no `WHERE` filter and a literal `LIMIT` of 10000 at most. For example, in `WHERE customer_id = :cid ORDER BY amount DESC LIMIT 10`, only `customer_id` gets an index.

A filter counts only when its column stands alone in the comparison. The compiler warns about a filter that wraps its column in an expression.

## Filters inside a subquery

The compiler selects the indexes for the filters of a subquery as for any query, with the columns of the outer query that the subquery compares with as parameters. How the database runs the subquery decides which tables get an index:

- `EXISTS (...)` and `IN (SELECT ...)` combined with the other conditions of the `WHERE` clause by `AND`: the database joins the two tables and can start from either one. The subquery's table gets an index on the columns that it compares with a parameter, followed by the columns that it compares with the outer query. The outer table gets an index on the column that the subquery compares with.
- `NOT EXISTS (...)`, `NOT IN (SELECT ...)`, a subquery that is part of an `OR`, and a scalar subquery such as `(SELECT COUNT(*) ...)`: the database runs the subquery once, or for each row of the outer table. Only the subquery's table gets an index. The database reads every row of the outer table that the other conditions leave.

Example: the API returns the orders of all customers who own a given account. `AccountOwners` has the primary key `owner_id`, and a customer can own an account through several of its rows, so a `JOIN` would add duplicate rows.

```sql
-- AccountOwners gets an index on (account_id, customer_id), Orders on customer_id
OrdersByAccount(accountId STRING) := SELECT * FROM Orders c
WHERE EXISTS (SELECT 1 FROM AccountOwners o
              WHERE o.account_id = :accountId AND o.customer_id = c.customer_id);
```

## Index hints

Add an `index` hint only to a table that needs it. An `index` hint replaces the automatic index selection for its whole table: the compiler then creates only the hinted indexes, the primary key and, when the table backs a paginated query, a `BTREE` index on its row-time column, and no longer warns about that table. So also add to the hint each index in the `Table` section of that table that another served query still uses. The row-time index needs no hint. Write it as `index(TYPE, column, ...)` hint, not as the `CREATE INDEX` statement: `index Orders_2_btree_c2dc0 (btree): time DESC, id` becomes `index(BTREE, time DESC, id)`. The hint is not limited to the maximum number of indexes that the full-table-scan warning names.

After the compile, check the indexes of the other served queries of that table (see Checking the indexes).

If the table comes from an included project (`script.include`), the `index` hint belongs in that project. If you cannot change that project, report it to its owners.
