Could not find field when trying to resolve expression.

Erroneous code example:
```
IMPORT ecommerce-data.Orders;
Orders.x := SELECT missingField FROM @;
```

Assure the column name exists.