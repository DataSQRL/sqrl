To-many path not allowed in field path in this location.

Erroneous code example:
```
IMPORT ecommerce-data.Orders;
Orders.x := SELECT entries.discount FROM @;
```

Maybe try moving the to-many field to the FROM clause. Example:
```
IMPORT ecommerce-data.Orders;
Orders.x := SELECT discount FROM @.entries;
```
