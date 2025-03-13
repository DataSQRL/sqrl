Invalid SQRL definition. Expected one of SELECT, DISTINCT, or column expression.

To define a table use:
```
Table := SELECT * FROM AnotherTable;
```
To deduplicate a stream use:
```
DistinctCustomers := DISTINCT Customers ON customerId ORDER BY lastUpdated DESC;
```
To define a table function use:
```
OlderCustomers(age INT) := SELECT * FROM Customers WHERE age >= :age;
```
To define a relationship use:
```
Customers.orders := SELECT * FROM Orders o WHERE o.customerid = this.id ORDER BY o.orderTime DESC;
```
To add a column to an existing table that is defined immediately above, use:
```
Customers.full_name := CONCAT(first_name, last_name);
```