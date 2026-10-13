The definition statement (`Name := ...`) is invalid.

Common causes: a name with more than two path segments, `SUBSCRIBE` on a relationship, `this` outside a relationship definition, arguments on a `DISTINCT` or column-addition statement, or a malformed `DISTINCT` statement.

Erroneous code example:
```sql
Customer.orders.items := SELECT * FROM OrderItems i WHERE this.id = i.orderid;
```

Relationships are always defined on the table they start from:
```sql
Customer.orders := SELECT * FROM Orders o WHERE this.customerid = o.customerid;
Orders.items := SELECT * FROM OrderItems i WHERE this.id = i.orderid;
```

Valid definitions are tables (`T := SELECT ...;`), deduplications (`T := DISTINCT Src ON id ORDER BY ts DESC;`), functions (`F(arg INT) := SELECT ... WHERE col = :arg;`), relationships (`T.rel := SELECT ... WHERE this.id = x.tid;`), and column additions (`T.col := expression;`).
