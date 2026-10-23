# Version Compatibility

DataSQRL builds on top of Flink and the Flink connector ecosystem.

| DataSQRL | Flink SQL | Flink Runtime                | Postgres             | pgvector | Iceberg                    | Kafka                  |  
|----------|-----------|------------------------------|----------------------|----------|----------------------------|------------------------|
| 0.6.x    | 1.19.x    | 1.19+ <br /> 1.19.1+2 tested | 13+ <br /> 14 tested | 0.8.0    | 1.9+ <br /> 1.9.0 tested   | 3+ <br /> 3.4.0 tested |
| 0.7.x    | 1.19.x    | 1.19+ <br /> 1.19.2+3 tested | 15+ <br /> 17 tested | 0.8.0    | 1.9+ <br /> 1.9.0 tested   | 3+ <br /> 3.4.0 tested |
| 0.8.x    | 1.19.x    | 1.19+ <br /> 1.19.3 tested   | 15+ <br /> 17 tested | 0.8.0    | 1.9+ <br /> 1.9.2 tested   | 3+ <br /> 3.4.0 tested |
| 0.9.x    | 2.2.x     | 2.2+ <br /> 2.2.0 tested     | 17+ <br /> 17 tested | 0.8.0    | 1.10+ <br /> 1.10.0 tested | 3+ <br /> 3.9.1 tested |
| 0.10.x   | 2.2.x     | 2.2+ <br /> 2.2.1 tested     | 17+ <br /> 18 tested | 0.8.0    | 1.10+ <br /> 1.11.0 tested | 3+ <br /> 4.2.0 tested |
| 0.11.x   | 2.3.x     | 2.3+ <br /> 2.3.0 tested     | 17+ <br /> 18 tested | 0.8.0    | 1.11+ <br /> 1.11.0 tested | 3+ <br /> 4.2.0 tested |

## Upgrade Notes

### Upgrading from 0.11

DataSQRL 0.12.2+ computes the generated `__pk_hash` primary key column with `hash_key` instead of `hash_columns`, which 0.11 used.
This column is the primary key of the database tables for stream tables without a primary key, where it hashes all columns, and for tables with nullable primary key columns, where it hashes the key columns.
`hash_columns` could give distinct rows the same key, so one of them was dropped, and it gave a row with an `ARRAY` or `BYTES` column a different key each time the row was processed.

Rows that a 0.11 deployment already stored keep the keys computed by `hash_columns`, so they do not conflict with rows written after the upgrade:

- **Stream tables without a primary key:** rows that are processed again after the upgrade are stored a second time, for example when the job starts without a savepoint, restores an older checkpoint, or Kafka offsets are reset. Restore the job from a savepoint taken when stopping the 0.11 job, or rebuild these tables as well.
- **Tables with nullable primary key columns:** the first update of an existing row after the upgrade stores a new row, and the old row stays. Rebuild these tables by emptying them and processing their input again from the start.

The key change itself only renames the function in the compiled Flink plan, so it does not affect whether a savepoint can be restored. Compare `flink-compiled-plan.json` between the versions to check the other changes.
