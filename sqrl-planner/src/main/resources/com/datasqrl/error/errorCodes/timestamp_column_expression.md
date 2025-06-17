Timestamp column is an expression but missing a column name.

Erroneous code example:
```
IMPORT ecommerce-data.Orders TIMESTAMP time/1000;
```

Set a column name using the AS keyword. Example:
```
IMPORT ecommerce-data.Customer TIMESTAMP time/1000 AS ts;
```
