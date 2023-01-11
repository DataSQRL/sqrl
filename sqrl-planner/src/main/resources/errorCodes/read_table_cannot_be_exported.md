Only tables that are computed in-stream can be exported.
The optimizer determines which tables are computed in-stream based on a cost-model.

Use the optimizer hint `/*+ EXEC_STREAM */` on the exported table to force the
optimizer to compute the table in-stream.

For example:
```
/*+ EXEC_STREAM */
MyJoinTable := SELECT t1.id, t2.location FROM MyTable1 t1 JOIN MyTable2 t2 ON t1.name = t2.name 

EXPORT MyJoinTable TO print.MyJoinTable
```

For a table to be computed in-stream, all tables in the `FROM` clause must also 
be computed in-stream which may require applying the optimizer hint to multiple tables.

**Warning:**
When the optimizer determines a table should not be computed in-stream it may be too
expensive to do so. Using the optimizer hint to force in-stream computation may result
in slow performance, unresponsiveness, or out-of-memory exceptions.