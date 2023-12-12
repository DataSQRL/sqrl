/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Retail;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

class FlinkPhysicalIT extends AbstractPhysicalSQRLIT {

  private Retail example = Retail.INSTANCE;
  private Path exportPath = example.getRootPackageDirectory().resolve("export-data");

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getFlinkWithDB(), (Path) null, Optional.of(exportPath));
    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    if (!Files.isDirectory(exportPath)) {
      Files.createDirectory(exportPath);
    }
  }

  @AfterEach
  @SneakyThrows
  public void cleanupDirectory() {
    super.tearDown();
    //Contents written to exportPath are validated in validateTables()
    if (Files.isDirectory(exportPath)) {
      FileUtils.deleteDirectory(exportPath.toFile());
    }
  }

  @Test
  public void tableImportTest() {
    String script = example.getImports().toString();
    validateTables(script, "customer", "product", "orders", "entries");
  }

  @Test
  public void tableColumnDefinitionTest() {
    ScriptBuilder builder = example.getImports();


    builder.add("Customer.timestamp := epochToTimestamp(lastUpdated)");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY timestamp DESC");

    builder.add("Orders.col1 := (id + customerid)/2");
    builder.add("Orders.entries.discount2 := COALESCE(discount,0.0)");

    builder.add(
            "EntryPrice := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e"); //This is line 4 in the script

    builder.add(
        "OrderCustomer := SELECT o.id, c.name, o.customerid, o.col1, e.discount2 FROM Orders o JOIN o.entries e JOIN Customer c on o.customerid = c.customerid");

    validateTables(builder.getScript(), "entryprice", "customer", "orders", "entries",
        "ordercustomer");
  }

  @Test
  public void jsonToString() {
    ScriptBuilder builder = example.getImports();
    builder.add("IMPORT json.*");
    builder.add("IMPORT string.*");
    builder.add("BigJoin := SELECT * FROM Customer JOIN Product on true");
    builder.add("Array := SELECT jsonArray(customerid) AS obj FROM BigJoin");
    builder.add("Agg := SELECT jsonObjectAgg('key', name) AS agg FROM BigJoin GROUP BY name");
    builder.add("ToString := SELECT jsonToString(toJson('{}')) AS obj FROM BigJoin");
    builder.add("Ext := SELECT jsonExtract(toJson('{\"a\": \"hello\"}'), CAST('$.a' AS varchar), CAST('default' AS varchar)) AS obj FROM BigJoin");
    builder.add("Query := SELECT jsonQuery(toJson('{\"a\": {\"b\": 1}}'), '$.a') AS obj FROM BigJoin");
    builder.add("Exist := SELECT jsonExists(toJson('{\"a\": true}'), '$.a') AS obj FROM BigJoin");
    builder.add("ConcatJson := SELECT jsonConcat(toJson('{\"a\": true}'), toJson('{\"a\": false}')) AS obj FROM BigJoin");
    builder.add("ArrayAgg := SELECT jsonArrayAgg(name) AS agg FROM BigJoin GROUP BY name");
    builder.add("ObjComplex := SELECT jsonObject(concat('application#',CAST(name AS VARCHAR)), customerid) AS obj FROM BigJoin");

    validateTables(builder.getScript(), "Array", "Agg", "ToString", "Ext", "Query", "Exist", "ConcatJson", "ArrayAgg", "ObjComplex");
  }

  @Test
  public void functionTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("IMPORT text.*");
    builder.add("Product.badWords := bannedWordsFilter(name)");
    builder.add("Product.searchResult := textsearch('garden gnome', description, name)");
    builder.add("Product.format := format('Go buy: %s in %s with id=%s', name, category, CAST(productid AS STRING))");
    validateTables(builder.getScript(), "product");
  }

  @Test
  public void nestedAggregationandSelfJoinTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT time.*");
    builder.add("IMPORT ecommerce-data.Orders");
    builder.add(
        "IMPORT ecommerce-data.Customer TIMESTAMP epochToTimestamp(lastUpdated) AS updateTime");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add(
        "Orders.total := SELECT SUM(e.quantity * e.unit_price - e.discount) as price, COUNT(e.quantity) as num, SUM(e.discount) as discount FROM @.entries e");
    builder.add(
        "OrdersInline := SELECT o.id, o.customerid, o.time, t.price, t.num FROM Orders o JOIN o.total t");
//    builder.add("Customer.orders_by_day := SELECT o.time, o.price, o.num FROM @ JOIN OrdersInline o ON o.customerid = @.customerid");
    builder.add(
        "Customer.orders_by_hour := SELECT endOfHour(o.time) as hour, SUM(o.price) as total_price, SUM(o.num) as total_num FROM @ JOIN OrdersInline o ON o.customerid = @.customerid GROUP BY hour");
    validateTables(builder.getScript(), "customer", "orders", "ordersinline", "orders_by_hour");
  }

  @Test
  public void joinTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT time.*");
    builder.add(
        "IMPORT ecommerce-data.Customer TIMESTAMP epochToTimestamp(lastUpdated) as updateTime"); //we fake that customer updates happen before orders
    builder.add("IMPORT ecommerce-data.Orders TIMESTAMP time AS rowtime");

    //Normal join
    builder.add(
        "OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid");
    //Interval join
    builder.add(
        "OrderCustomerInterval := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "
            +
            "AND o.rowtime >= c.updateTime AND o.rowtime <= c.updateTime + INTERVAL 365 DAYS");
    //Temporal join
    builder.add("CustomerDedup := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add(
        "OrderCustomerDedup := SELECT o.id, c.name, o.customerid FROM Orders o JOIN CustomerDedup c on o.customerid = c.customerid");

    validateTables(builder.getScript(), "ordercustomer", "ordercustomerinterval", "customerdedup",
        "ordercustomerdedup");
  }

  @Test
  public void aggregateTest() {
    ScriptBuilder builder = example.getImports();
    //temporal state
    builder.append(
        "OrderAgg1 := SELECT o.customerid as customer, endOfHour(o.time, 1, 15) as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    builder.append("OrderAgg2 := SELECT COUNT(o.id) as order_count FROM Orders o");
    //time window
    builder.append(
        "Ordertime1 := SELECT o.customerid as customer, endOfSecond(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    //now() and sliding window
    builder.append(
        "OrderNow1 := SELECT o.customerid as customer, endOfHour(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.time > NOW() - INTERVAL 999 DAYS) GROUP BY customer, bucket");
    builder.append(
        "OrderNow2 := SELECT endOfHour(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.time > NOW() - INTERVAL 999 DAYS) GROUP BY bucket");
    builder.append(
        "OrderNow3 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o  WHERE (o.time > NOW() - INTERVAL 999 DAYS) GROUP BY customer");
    builder.append(
        "OrderAugment := SELECT o.id, o.time, c.order_count FROM Orders o JOIN OrderNow3 c ON o.customerid = c.customer"); //will be empty because OrderNow3 has a timestamp greater than Order
    //state
    builder.append(
        "OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    builder.append(
        "agg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");

    validateTables(builder.getScript(), Set.of("ordernow3"), "orderagg1", "orderagg2", "ordertime1",
        "ordernow1",
        "ordernow2", "ordernow3", "orderaugment", "ordercustomer", "agg1");
  }

  @Test
  public void filterTest() {
    ScriptBuilder builder = example.getImports();

    builder.add("HistoricOrders := SELECT * FROM Orders WHERE time >= now() - INTERVAL 999 DAYS");
    builder.add("RecentOrders := SELECT * FROM Orders WHERE time >= now() - INTERVAL 1 SECOND");

    validateTables(builder.getScript(), "historicorders", "recentorders");
  }

  @Test
  public void topNTest() {
    ScriptBuilder builder = example.getImports();
    topNTest(builder);
  }

  public void topNTest(ScriptBuilder builder) {
    builder.add("Customer.updateTime := epochToTimestamp(lastUpdated)");
    builder.add("CustomerDistinct := DISTINCT Customer ON customerid ORDER BY updateTime DESC;");
    builder.add(
        "CustomerDistinct.recentOrders := SELECT o.id, o.time  FROM @ JOIN Orders o WHERE @.customerid = o.customerid ORDER BY o.time DESC LIMIT 10;");

    builder.add("CustomerId := SELECT DISTINCT customerid FROM Customer;");
    builder.add(
        "CustomerOrders := SELECT o.id, c.customerid FROM CustomerId c JOIN Orders o ON o.customerid = c.customerid");

    builder.add(
        "CustomerDistinct.distinctOrders := SELECT DISTINCT o.id FROM @ JOIN Orders o WHERE @.customerid = o.customerid ORDER BY o.id DESC LIMIT 10;");
    builder.add(
        "CustomerDistinct.distinctOrdersTime := SELECT DISTINCT o.id, o.time  FROM @ JOIN Orders o WHERE @.customerid = o.customerid ORDER BY o.time  DESC LIMIT 10;");

    builder.add("Orders := DISTINCT Orders ON id ORDER BY time DESC");

    validateTables(builder.getScript(), "customerdistinct", "customerid", "customerorders",
        "distinctorders", "distinctorderstime", "orders", "entries");
  }

  @Test
  public void fuzzedValuesTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.append("IMPORT ecommerce-data-large.Customer");
    builder.append("IMPORT ecommerce-data-large.Orders");
    builder.append("IMPORT ecommerce-data-large.Product");
    builder.append("IMPORT time.*");

    topNTest(builder);
  }

  @Test
  public void setTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT time.*");
    builder.add(
        "IMPORT ecommerce-data.Customer TIMESTAMP epochToTimestamp(lastUpdated) as updateTime"); //we fake that customer updates happen before orders
    builder.add("IMPORT ecommerce-data.Orders");

    builder.add("CombinedStream := SELECT o.customerid, o.time AS rowtime FROM Orders o" +
        " UNION ALL " +
        "SELECT c.customerid, c.updateTime AS rowtime FROM Customer c;");
    builder.add(
        "StreamCount := SELECT endOfHour(rowtime) as hour, COUNT(1) as num FROM CombinedStream GROUP BY hour");
    validateTables(builder.getScript(), "combinedstream", "streamcount");
  }

  @Test
  public void streamTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer.updateTime := epochToTimestamp(lastUpdated)");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add(
        "CustomerCount := SELECT c.customerid, c.name, SUM(e.quantity) as quantity FROM Orders o JOIN o.entries e JOIN Customer c on o.customerid = c.customerid GROUP BY c.customerid, c.name");
    builder.add(
        "CountStream := STREAM ON ADD AS SELECT customerid, name, quantity FROM CustomerCount WHERE quantity > 1");
    builder.add(
        "UpdateStream := STREAM ON UPDATE AS SELECT customerid, name, quantity FROM CustomerCount WHERE quantity > 1");
    builder.add("CustomerCount2 := DISTINCT CountStream ON customerid ORDER BY _source_time DESC");
    builder.add("EXPORT CountStream TO print.CountStream");
    builder.add("EXPORT CountStream TO output.CountStream");
    validateTables(builder.getScript(), "customercount", "countstream", "customercount2","updatestream");
  }

  @Test
  public void simpleStreamTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Orders");
    builder.add("CountStream0 := STREAM ON ADD AS SELECT customerid, count(1) AS num FROM orders GROUP BY customerid");
    builder.add("CustomerCount := SELECT o.customerid, count(1) as quantity FROM Orders o GROUP BY o.customerid");
    builder.add("CountStream1 := STREAM ON ADD AS SELECT customerid, quantity FROM CustomerCount WHERE quantity >= 1");
    builder.add("CountStream2 := STREAM ON ADD AS SELECT customerid, quantity FROM CustomerCount WHERE quantity >= 2");
    builder.add("UpdateStream0 := STREAM ON UPDATE AS SELECT customerid, count(1) AS num FROM orders GROUP BY customerid");
    builder.add("UpdateStream1 := STREAM ON UPDATE AS SELECT customerid, quantity FROM CustomerCount WHERE quantity >= 1");
    validateTables(builder.getScript(), "customercount", "countstream0", "countstream1",
        "countstream2","updatestream0","updatestream1");
  }
}
