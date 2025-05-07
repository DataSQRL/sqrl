Table path cannot be found.

Erroneous code example:
```
IMPORT ecommerce-data.Orders;
Orders.x := SELECT * FROM @.entrees;
____________________________^
```

Assure the table path exists. Example:
```
IMPORT ecommerce-data.Orders;
Orders.x := SELECT * FROM @.entries;
```
