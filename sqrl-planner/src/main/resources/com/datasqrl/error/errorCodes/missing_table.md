A query references a table or function that does not exist.

Erroneous code example:
```sql
IMPORT connectors.sources.*;

HighValueOrders := SELECT * FROM Ordrs WHERE amount > 100;
```

Check the spelling of the table name and that the table is imported or defined *before* the statement that uses it. Tables imported into a namespace must be referenced with that namespace, e.g. `src.Orders` after `IMPORT connectors.sources AS src;`:
```sql
IMPORT connectors.sources.*;

HighValueOrders := SELECT * FROM Orders WHERE amount > 100;
```
