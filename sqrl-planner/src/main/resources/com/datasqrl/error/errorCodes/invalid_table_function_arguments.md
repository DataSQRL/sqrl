The arguments of a function or relationship definition are invalid.

Arguments are declared like columns in `CREATE TABLE` (name, type, optional `NOT NULL` or `METADATA FROM '...'`) and referenced in the query as `:name`. Every referenced argument must be declared, and a relationship must be defined on a previously defined table.

Erroneous code example:
```sql
CustomerByEmail(email STRING) := SELECT * FROM Customer WHERE email = :mail;
```

Reference only declared arguments:
```sql
CustomerByEmail(email STRING) := SELECT * FROM Customer WHERE email = :email;
```
