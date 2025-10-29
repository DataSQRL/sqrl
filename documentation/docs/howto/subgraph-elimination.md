# Subgraph Elimination

Sometimes the Flink optimizer is too smart for its own good and will push down predicates that make common subgraph identification impossible resulting in duplicate computation.
That can result in much larger job graphs and poor performance or high state maintenance.

To inhibit predicate pushdown, SQRL provides the `noop` function that takes an arbitrary list of argument and always returns true.
As such, the function serves no purpose other than making it impossible for the optimizer to push down predicates.

Consider the following schematic example:

```sql
MyComputedTable := SELECT a, b, expensive_function(data) AS c FROM InputData;

ResultA := SELECT a, c FROM MyComputedTable WHERE noop(a,b,c);
ResultB := SELECT b, c FROM MyCOmputedTable WHERE noop(a,b,c);
```

Because `ResultA` and `ResultB` select different subsets of columns, those selections can get optimized down to the source `InputData` table resulting in `expensive_function` being executed twice because the relational trees are slightly different.
By adding the `noop` function we inhibit that push-down optimization.