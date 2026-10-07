The `primary_key` hint declares a primary key that the query does not guarantee to be unique.

The hint neither deduplicates the data nor is visible to Flink, so joins reading the table
keep whole rows in state instead of one row per key.

This typically happens when a function is applied to a primary key column of a state table,
because a function like `TRIM`, `LOWER`, or `CAST` can turn two different keys into the same value:

```sql
/*+primary_key(customer_id) */
CleanCustomer := SELECT TRIM(customer_id) AS customer_id, email FROM Customer;
```

To fix this, make the key unique with `DISTINCT ... ON` the key:

```sql
_CleanCustomer := SELECT TRIM(customer_id) AS customer_id, email, event_time FROM Customer;

CleanCustomer := DISTINCT _CleanCustomer ON customer_id ORDER BY event_time DESC;
```

Better yet, clean the key before the first table that has it as primary key, and select it
unchanged in every later table.
