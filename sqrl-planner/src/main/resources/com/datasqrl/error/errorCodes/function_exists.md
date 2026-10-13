A function or relationship with the same name is already defined.

SQRL does not support function overloading, i.e. multiple functions or relationships with the same name but different arguments.

Erroneous code example:
```sql
CustomerByEmail(email STRING) := SELECT * FROM Customer WHERE email = :email;
CustomerByEmail(email STRING, active BOOLEAN) := SELECT * FROM Customer WHERE email = :email AND active = :active;
```

Give each function or relationship a unique name:
```sql
CustomerByEmail(email STRING) := SELECT * FROM Customer WHERE email = :email;
CustomerByEmailAndStatus(email STRING, active BOOLEAN) := SELECT * FROM Customer WHERE email = :email AND active = :active;
```
