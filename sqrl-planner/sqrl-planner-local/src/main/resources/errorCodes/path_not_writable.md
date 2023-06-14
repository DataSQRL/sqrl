Cannot create assignment. Path does not target a writable relation. 

Erroneous code example:
```
IMPORT ecommerce-data.Customer;
IMPORT ecommerce-data.Orders;
Customer.orders := JOIN Orders ON Orders.customerid = @.customerid;

Customer.orders.entries.discount := discount ? 0.0;
_________^ cannot contain a join declaration or parent reference
```

Assign the column to the original type. Example:
```
IMPORT ecommerce-data.Customer;
IMPORT ecommerce-data.Orders;
Customer.orders := JOIN Orders ON Orders.customerid = @.customerid;

Orders.entries.discount := discount ? 0.0;
```
