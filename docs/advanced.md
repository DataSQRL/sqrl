

## User Defined Functions

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


### Java

### Python

## Script Imports

