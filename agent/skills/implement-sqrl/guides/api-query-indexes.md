# Indexes for API Queries

Read this guide only in the cases that implement-sqrl `SKILL.md`, "Indexes for API queries", names.

The compiler selects the indexes for most served queries. Fix only the queries that the case names. Add an `index` hint only to a table that the fix needs it for. Every other table keeps the indexes that the compiler selects.

## Fixing a slow query

1. Find the SQRL definition of each query to fix:
   - A slow-query log names a query by its API field. A top-level path, such as `/OrdersByCustomer`, is the name of the SQRL table or table function. A nested path, such as `/Customers[0]/orders`, names the relationship `orders` of `Customers`. A client alias replaces the field name in the path.
   - For an index warning, including the filter patterns that it lists with a full table scan, read "Index warnings" first.
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
2. Compare the columns that the query compares with a parameter or with another table, in the `WHERE` clause, in `JOIN ... ON` conditions and in subqueries, with the leading columns of the indexes. In an `EXISTS (...)` or `IN (SELECT ...)` that is not part of an `OR`, the column of the outer table that the subquery compares with counts too. A table of at most 100 rows needs no index (see Automatic index selection).
3. Make sure that each of these columns stands alone in the SQL. For a filter that wraps its column in an expression, such as `day_index + 1 = :day`, the compiler creates no index and warns `The filter ... cannot use an index on table ...`. When the filtered column comes from a table that runs in Postgres, the warning shows the expression of that table's query, such as `CAST(event_time AS STRING) = :t` for its column `t_str`.

## Automatic index selection

The compiler selects the indexes from these parts of the served queries:

- The filters of `WHERE` clauses, in table functions, relationships and `query_by_*` arguments.
- `JOIN ... ON` conditions, only for the table on the right side of the `JOIN`. The columns of the left table become parameters, and the left table gets no index from the join (see implement-sqrl `SKILL.md`, "Filters in API queries").
- Subqueries, such as `EXISTS (...)`, `IN (SELECT ...)` or `(SELECT COUNT(*) ...)` (see Filters inside a subquery).
- `text_search(...) > 0` filters: a `TEXT` index (GIN in Postgres) on the searched columns (see implement-sqrl `functions/text.md`).
- The first `ORDER BY` column, only when the query has no `WHERE` filter and a literal `LIMIT` of 10000 at most. For example, in `WHERE customer_id = :cid ORDER BY amount DESC LIMIT 10`, only `customer_id` gets an index.

A filter counts only when its column stands alone in the comparison. The compiler warns about a filter that wraps its column in an expression.

A table that holds at most 100 rows (`small-table-row-limit` in the `postgres` engine configuration) gets no automatic index and no warning, because the database reads it in about one page. The compiler takes the size from the definition of the table, like a `VALUES` list, or from a `row_count` hint. It does not count a table that reads another table, even a small one.

## Distinct counts

A `row_count` hint with column names gives the number of distinct values of those columns: `row_count(email, 1e6)`. Without it, the compiler assumes that an equality on a column matches 5% of the rows. Add the hint to a stored table when the requirements or the source data give the number. Write a number that you know. Leave the hint out when you would have to guess.

The compiler leaves an equality column out of the indexes when a distinct count shows that it narrows the rows too little. The limit is 20 (`min-distinct-count` in the `postgres` engine configuration):

- A column with fewer than 20 distinct values, such as `row_count(gender, 2)`. `email = :email AND gender = :gender` gets an index on `email` alone. A filter on `gender` alone gets no index and no warning.
- Every column beyond a unique column set. This takes two hints together, the row count of the table and an equal distinct count of the columns: `row_count(1e6), row_count(email, 1e6)`. An equality on `email` then matches one row, and the query gets an index on `email` alone.
- A column that multiplies the distinct values of the other equality columns by less than 20. With `row_count(country, 200), row_count(country, city, 2e3)`, `country = :country AND city = :city` gets an index on `country` alone.

A distinct count of several columns, such as `row_count(country, city, 2e3)`, applies only to a filter with an equality on all of those columns. For a filter on `country` alone, also write `row_count(country, 200)`.

A column without a distinct count and a column with `<`, `<=`, `>` or `>=` stay in the indexes, unless a unique column set makes them unnecessary. The leading primary key columns that the filter has an equality on always stay.

When a served query must have an index on a column that the compiler leaves out, add an `index` hint (see Index hints).

## Index warnings

The compiler warns when the filter patterns of a table need more indexes than it creates. Each of these warnings is about query speed only: the queries return the same rows with any set of indexes.

- `Created the maximum of 8 indexes for table ...`: the table needs more indexes than `max-indexes`. The warning lists the indexes that the limit omits, in the form of the `index` hint, such as ``index(HASH, `currency`)``, and the filter patterns that are left with a full table scan.
- `... need 12 indexes which exceeds the maximum of 8. Created a single-column index for each filtered column instead ...`: the table has one index for each filtered column and no composite index. The database combines these indexes at query time.
- `Table ... is queried with 60 distinct filter patterns which exceeds the maximum of 50`: the same result, for a table with very many filter patterns.

After such a warning:

1. Keep the served queries as they are. The requirements define them. Remove a filter or a query only when the requirements no longer need it.
2. Read the indexes of the table in `pipeline_explain.txt` (see Checking the indexes).
3. When the requirements, the plan or a review issue name a query of that table as slow or as important, add the `index` hints that this query needs (see Index hints). Otherwise leave the indexes to the compiler and report the warning.

## Filters inside a subquery

The compiler selects the indexes for the filters of a subquery as for any query, with the columns of the outer query that the subquery compares with as parameters. How the database runs the subquery decides which tables get an index:

- `EXISTS (...)` and `IN (SELECT ...)` combined with the other conditions of the `WHERE` clause by `AND`: the database joins the two tables. The subquery's table gets an index on the columns that it compares with a parameter, followed by the columns that it compares with the outer query. When the subquery compares with the outer columns only by `=`, as an `IN` always does, the database can start from either table, and the outer table gets an index on the column that the subquery compares with. With any other comparison, like `o.ts > c.ts`, the database starts from the outer table, which gets no index from the subquery.
- `NOT EXISTS (...)`, `NOT IN (SELECT ...)`, a subquery that is part of an `OR`, and a correlated scalar subquery such as `(SELECT COUNT(*) ...)`: the database runs the subquery once, or for each row of the outer table. Only the subquery's table gets an index. The database reads every row of the outer table that the other conditions leave.

A comparison with an uncorrelated scalar subquery, such as `customer_id = (SELECT MAX(customer_id) FROM ...)`, counts as a comparison with a parameter: `customer_id` gets an index.

Example: the API returns the orders of all customers who own a given account. `AccountOwners` has the primary key `owner_id`, and a customer can own an account through several of its rows, so a `JOIN` would add duplicate rows.

```sql
-- AccountOwners gets an index on (account_id, customer_id), Orders on customer_id
OrdersByAccount(accountId STRING) := SELECT * FROM Orders c
WHERE EXISTS (SELECT 1 FROM AccountOwners o
              WHERE o.account_id = :accountId AND o.customer_id = c.customer_id);
```

## Index hints

Add an `index` hint only to a table that needs it. An `index` hint replaces the automatic index selection for its whole table: the compiler then creates only the hinted indexes, the primary key and, when the table backs a paginated query, a `BTREE` index on its row-time column, and no longer warns about that table. So also add to the hint each index in the `Table` section of that table that another served query still uses. The row-time index needs no hint. Write it as `index(TYPE, column, ...)` hint, not as the `CREATE INDEX` statement: `index Orders_2_btree_c2dc0 (btree): time DESC, id` becomes `index(BTREE, time DESC, id)`. The hint is not limited to the maximum number of indexes that an index warning names. An index that a warning lists as omitted is already in the form of the hint.

After the compile, check the indexes of the other served queries of that table (see Checking the indexes).

If the table comes from an included project (`script.include`), the `index` hint belongs in that project. If you cannot change that project, report it to its owners.
