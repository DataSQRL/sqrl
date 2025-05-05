The table cannot be written to the database because it doesn't have a
primary key (required to identify each row) or timestamp (required to
ensure data consistency).

Usually, DataSQRL automatically infers the primary key and timestamp, but
we could not make an accurate inference for this table.

To fix this, you have two options:
1. Add an explicit primary key and timestamp after the table definition (preferred option).
2. Compute the table in the database instead of writing it to the database.

If you think DataSQRL should have been able to infer the primary key and timestamp, we apologize. Please submit a bug report and we'll fix it.