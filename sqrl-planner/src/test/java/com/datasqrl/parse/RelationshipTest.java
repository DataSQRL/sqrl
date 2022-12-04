/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.parse;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RelationshipTest {

  @Test
  public void testJoin1() {
    String s = "SELECT * FROM @;"; //self
    s = "Orders.x := SELECT * FROM @ a;"; //self alias
    //Trivial, just a table name
    s = "Orders.x := SELECT * FROM @.entries;"; //child
    s = "Orders.x := SELECT * FROM @.entries e;"; //child alias

    s = "Orders.x := SELECT * FROM @ JOIN @.entries;"; //child alias + condition
    s = "Orders.x := SELECT * FROM @ JOIN @.entries e;"; //child alias + condition
    s = "Orders.x := SELECT * FROM @ JOIN @.entries e ON e.quantity > 5;"; //child alias + condition

    s = "Orders.x := SELECT * FROM Orders"; //base table
    s = "Orders.x := SELECT * FROM Orders JOIN Orders.entries"; //base table + join
    s = "Orders.x := SELECT * FROM Orders o JOIN o.entries"; //base table + join alias
    s = "Orders.x := SELECT * FROM Orders.entries o JOIN o.parent"; //lopsided join
    s = "Orders.x := SELECT * FROM Orders o JOIN o.entries.parent"; //lopsided join
    s = "Orders.x := SELECT * FROM Orders.entries o JOIN Orders.entries o2"; //lopsided join

    s = "Orders.entries := SELECT * FROM @.parent;"; //parent
    s = "Orders.entries := SELECT * FROM @.parent p;"; //parent alias

    s = "Orders.entries := SELECT * FROM @ JOIN @.parent;"; //parent alias + condition
    s = "Orders.entries := SELECT * FROM @ JOIN @.parent p;"; //parent alias + condition
    s = "Orders.entries := SELECT * FROM @ JOIN @.parent p ON @.time > 100000;"; //parent alias + condition

    s = "Orders.total := count(entries);";
    s = "Orders.total := sum(entries.quantity);";
    s = "Orders.total := sum(entries.quantity) * sum(entries.unit_price);";

    s = "Orders.total := sum(entries.parent.entries.quantity);";

    s = "Orders.total := SELECT sum(entries.quantity) FROM @.entries.parent;";

    String join = "Customer.orders := JOIN Orders o ON o.customerid = @.customerid";
    //also for
    join = "Customer.orders := JOIN Orders o ON o.customerid = @.customerid LIMIT 1";

    s = "Customer.x := SELECT * FROM @.orders;";
    s = "Customer.x := SELECT * FROM @ JOIN @.orders;";
    s = "Customer.x := SELECT * FROM @ JOIN @.orders o ON o.quantity > 5;";
    s = "Customer.x := SELECT * FROM @ JOIN @.orders o ON @._ingest_time > 10000;";
    s = "Customer.x := SELECT * FROM @.orders.entries;";
    s = "Customer.x := SELECT * FROM @.orders.entries e JOIN e.parent p;";
    s = "Customer.x := SELECT * FROM Customer.orders.entries e;";

    s = "Customer.total := count(orders);";

    s = "Customer.total := sum(orders.entries.quantity);";
    s = "Customer.total := SELECT sum(entries.quantity) FROM @.orders;";
    s = "Customer.total := SELECT sum(orders.entries.quantity) FROM @;";
    s = "Customer.total := SELECT * FROM @.orders WHERE count(entries.quantity) > 5;";

    s = "Customer.total := SELECT sum(entries.quantity) FROM Customer.orders;";
  }
}