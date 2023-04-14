Could not find field when trying to resolve expression.

Erroneous code example:
```
IMPORT datasqrl.seedshop.Customers;
DistinctCustomers := DISTINCT Customers ON customerid ORDER BY name ASC;
```

`name` is not a valid timestamp column and the order direction must be `DESC`.

If DataSQRL does not recognize the column in the `ORDER BY` as a valid timestamp
column, try declaring it as a timestamp in the `IMPORT` statement:
```
IMPORT datasqrl.seedshop.Customers TIMESTAMP changed_on;
```