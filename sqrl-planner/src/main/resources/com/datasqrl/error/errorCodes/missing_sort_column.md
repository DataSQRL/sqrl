A table definition sorts by a column that is not part of its `SELECT` clause.

DataSQRL executes the `ORDER BY` of a table definition at query time, so every sort column must be in the result.

Erroneous code example:
```sql
RecentOrders := SELECT id, amount FROM Orders ORDER BY order_time DESC;
```

Add the sort column to the `SELECT` clause. Prefix it with `_` to hide it from the API:
```sql
RecentOrders := SELECT id, amount, order_time AS _order_time FROM Orders ORDER BY _order_time DESC;
```

Alternatively, define a function: functions keep their `ORDER BY` without selecting the sort columns.
