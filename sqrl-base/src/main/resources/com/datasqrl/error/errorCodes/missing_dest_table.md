Cannot find table for statement.

Erroneous code example:
```
IMPORT ecommerce-data.Orders;
Orders.entries.discount := discount ? 0.0;
_______^
```

Assure that the table path exists. Example:
```
IMPORT ecommerce-data.Orders;
Orders.entries.discount := discount ? 0.0;
```
