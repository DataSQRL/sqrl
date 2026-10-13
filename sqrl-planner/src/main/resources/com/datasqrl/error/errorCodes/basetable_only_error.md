A relationship was added to a table that is not a base table.

A table that selects from another table without changing its columns (e.g. it only filters rows) has that other table as its base table and shares its API result type. Relationships can only be defined on the base table.

Erroneous code example:
```sql
ActiveCustomer := SELECT * FROM Customer WHERE active;
ActiveCustomer.orders := SELECT * FROM Orders o WHERE this.customerid = o.customerid;
```

Define the relationship on the base table instead. Tables that share the base table's result type, like `ActiveCustomer`, expose it as well:
```sql
ActiveCustomer := SELECT * FROM Customer WHERE active;
Customer.orders := SELECT * FROM Orders o WHERE this.customerid = o.customerid;
```
