Invalid SQRL definition. Expected one of SELECT, DISTINCT, JOIN.

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
OlderCustomers($age: INT) := SELECT * FROM Customers WHERE age >= $age;
```
To define a relationship use:
```
Customers.orders := JOIN Orders o ON o.customerid = this.id ORDER BY o.orderTime DESC;
```