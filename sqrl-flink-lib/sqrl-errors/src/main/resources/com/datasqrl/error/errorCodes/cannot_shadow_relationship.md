Cannot shadow a relationship field.

Erroneous code example:
```
IMPORT ecommerce-data.Customer;
IMPORT ecommerce-data.Orders;
Customer.orders := JOIN Orders ON Orders.customerid = @.customerid;
Customer.orders := 1;
___________________^
```

Use a different name for the new column. Example:
```
IMPORT ecommerce-data.Customer;
IMPORT ecommerce-data.Orders;
Customer.orders := JOIN Orders ON Orders.customerid = @.customerid;
Customer.orders_1 := 1;
```
