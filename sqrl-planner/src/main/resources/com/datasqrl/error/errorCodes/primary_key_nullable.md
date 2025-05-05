Nullable columns are not allowed as part of a table's primary key.

The primary key uniquely identifies each row in a table. DataSQRL
automatically keeps track of the primary key columns, but for queries
with an OUTER join (e.g. a LEFT or RIGHT join) DataSQRL cannot infer
what the value of the primary key column should be when there is no 
record to join on the other side.

To fix this, explicitly set such a value using the `coalesce` function.

For example, suppose we are joining two tables `TableA` and `TableB` with
integer primary key columns `colA` and `colB` respectively as follows:

```sql
TableC := SELECT colA, colB FROM TableA LEFT JOIN TableB;
```

This would lead to the error because `colB` can be null. To fix:

```sql
TableC := SELECT colA, coalesce(colB,0) FROM TableA LEFT JOIN TableB;
```

Note, that if you are not explicitly selecting the primary key column
mentioned in this error, you should add it to the SELECT list.