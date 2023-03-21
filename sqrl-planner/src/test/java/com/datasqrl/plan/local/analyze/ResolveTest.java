/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.analyze;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.name.Name;
import com.datasqrl.plan.calcite.table.AbstractRelationalTable;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.datasqrl.plan.calcite.table.PullupOperator;
import com.datasqrl.plan.calcite.table.QueryRelationalTable;
import com.datasqrl.plan.calcite.table.TableType;
import com.datasqrl.plan.calcite.table.TimestampHolder;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.TestRelWriter;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.commons.compress.utils.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class ResolveTest extends AbstractLogicalSQRLIT {
  protected SnapshotTest.Snapshot snapshot;
  private SqrlSchema schema;

  private Path exportPath = Retail.INSTANCE.getRootPackageDirectory().resolve("export-data");
  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), null, Optional.of(exportPath));
    schema = injector.getInstance(SqrlSchema.class);

    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    if (!Files.isDirectory(exportPath)) {
      Files.createDirectory(exportPath);
    }

  }

  @AfterEach
  @SneakyThrows
  public void tearDown() {
    super.tearDown();
    snapshot.createOrValidate();
    FileUtil.deleteDirectory(exportPath);
  }

  /*
  ===== IMPORT TESTS ======
   */

  @Test
  public void tableImportTest() {
    plan(imports().toString());
    validateQueryTable("customer", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.candidates(1));
    validateQueryTable("product", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.candidates(1));
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.candidates(1, 4));
  }

  /*
  ===== TABLE & COLUMN DEFINITION (PROJECT) ======
   */

  @Test
  public void timestampColumnDefinition() {
    String script = ScriptBuilder.of("IMPORT ecommerce-data.Customer",
        "IMPORT time.*",
        "Customer.timestamp := epochToTimestamp(lastUpdated)",
        "Customer.month := endOfMonth(endOfMonth(timestamp))",
        "CustomerCopy := SELECT timestamp, month FROM Customer");
    plan(script);
    validateQueryTable("customer", TableType.STREAM, ExecutionEngine.Type.STREAM, 8, 1,
        TimestampTest.candidates(1, 6, 7));
    validateQueryTable("customercopy", TableType.STREAM, ExecutionEngine.Type.STREAM, 4, 1,
        TimestampTest.candidates(1, 2, 3));
  }

  @Test
  public void timestampExpressionTest() {
    plan(
        "IMPORT time.*; IMPORT ecommerce-data.Customer TIMESTAMP epochToTimestamp(lastUpdated) AS timestamp;\n");
    validateQueryTable("customer", TableType.STREAM, ExecutionEngine.Type.STREAM, 7, 1,
        TimestampTest.fixed(6));
  }

  @Test
  public void timestampDefinitionTest() {
    plan("IMPORT ecommerce-data.Orders TIMESTAMP time;\n");
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.fixed(4));
  }

  @Test
  public void addingSimpleColumns() {
    String script = ScriptBuilder.of("IMPORT ecommerce-data.Orders",
        "Orders.col1 := (id + customerid)/2",
        "Orders.entries.discount2 := coalesce(discount,0.0)",
        "OrderEntry := SELECT o.col1, o.\"time\", e.productid, e.discount2 FROM Orders o JOIN o.entries e");
    plan(script);
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM, 7, 1,
        TimestampTest.candidates(1, 4));
    validateQueryTable("orderentry", TableType.STREAM, ExecutionEngine.Type.STREAM, 7, 2,
        TimestampTest.candidates(3, 6));
  }

  @Test
  public void selfJoinSubqueryTest() {
    ScriptBuilder builder = imports();
    builder.add("Orders2 := SELECT o2._uuid FROM Orders o2 "
        + "INNER JOIN (SELECT _uuid FROM Orders) AS o ON o._uuid = o2._uuid;\n");
    plan(builder.toString());
    validateQueryTable("orders2", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.candidates(1, 2));
  }

  @Test
  public void tableDefinitionTest() {
    String sqrl = ScriptBuilder.of("IMPORT ecommerce-data.Orders",
        "EntryCount := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;");
    plan(sqrl);
    validateQueryTable("entrycount", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 2,
        TimestampTest.candidates(3, 4)); //5 cols = 1 select col + 2 pk cols + 2 timestamp cols
  }

  /*
  ===== NESTED TABLES ======
   */

  @Test
  public void nestedAggregationAndSelfJoinTest() {
    String sqrl = ScriptBuilder.of("IMPORT ecommerce-data.Orders",
        "IMPORT ecommerce-data.Customer",
        "IMPORT time.*",
        "Customer := DISTINCT Customer ON customerid ORDER BY \"_ingest_time\" DESC",
        "Orders.total := SELECT SUM(e.quantity * e.unit_price - e.discount) as price, COUNT(e.quantity) as num, SUM(e.discount) as discount FROM @.entries e",
        "OrdersInline := SELECT o.id, o.customerid, o.\"time\", t.price, t.num FROM Orders o JOIN o.total t",
        "Customer.orders_by_day := SELECT endOfDay(o.\"time\") as day, SUM(o.price) as total_price, SUM(o.num) as total_num FROM @ JOIN OrdersInline o ON o.customerid = @.customerid GROUP BY day");
    plan(sqrl);
    validateQueryTable("total", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 1,
        TimestampTest.fixed(4));
    validateQueryTable("ordersinline", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.fixed(3));
    validateQueryTable("orders_by_day", TableType.STREAM, ExecutionEngine.Type.STREAM, 4, 2,
        TimestampTest.fixed(1));
  }


  /*
  ===== JOINS ======
   */

  @Test
  public void tableJoinTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    plan(builder.toString());
    validateQueryTable("ordercustomer", TableType.STATE, ExecutionEngine.Type.DATABASE, 5, 2,
        TimestampTest.NONE); //numCols = 3 selected cols + 2 uuid cols for pk
  }

  @Test
  public void tableIntervalJoinTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o INTERVAL JOIN Customer c on o.customerid = c.customerid "
            +
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    builder.add(
        "OrderCustomer2 := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "
            +
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    plan(builder.toString());
    validateQueryTable("ordercustomer", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 2,
        TimestampTest.fixed(5)); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
    validateQueryTable("ordercustomer2", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 2,
        TimestampTest.fixed(5)); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
  }

  @Test
  public void tableTemporalJoinTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "CustomerCount := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer");
    builder.add(
        "OrderWithCount := SELECT o.id, c.order_count, o.customerid FROM Orders o TEMPORAL JOIN CustomerCount c on o.customerid = c.customer");
    builder.add(
        "OrderWithCount2 := SELECT o.id, c.order_count, o.customerid FROM CustomerCount c TEMPORAL JOIN Orders o on o.customerid = c.customer");
    plan(builder.toString());
    validateQueryTable("orderwithcount", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 1,
        TimestampTest.fixed(4)); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
    validateQueryTable("orderwithcount2", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 1,
        TimestampTest.fixed(4)); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
  }

  @Test
  public void tableTemporalJoinWithTimeFilterTest() {
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC");
    builder.add("Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC");
    builder.add("Customer.orders := JOIN Orders ON Orders.customerid = @.customerid");
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid");
    builder.add("Customer.totals := SELECT p.category as category, sum(e.quantity) as num " +
        "FROM @.orders o JOIN o.entries e JOIN e.product p WHERE o.\"time\" >= now() - INTERVAL 1 YEAR GROUP BY category");
    plan(builder.toString());
    validateQueryTable("totals", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 4, 2,
        TimestampTest.fixed(3), PullupTest.builder().hasTopN(true).build());
  }


  /*
  ===== AGGREGATE ======
   */

  @Test
  public void streamAggregateTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "OrderAgg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer");
    builder.add("OrderAgg2 := SELECT COUNT(o.id) as order_count FROM Orders o;");
    plan(builder.toString());
    validateQueryTable("orderagg1", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build()); //timestamp column is added
    validateQueryTable("orderagg2", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
  }

  @Test
  public void streamTimeAggregateTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "Ordertime1 := SELECT o.customerid as customer, endOfsecond(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    plan(builder.toString());
    validateQueryTable("ordertime1", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 2,
        TimestampTest.fixed(1));
  }

  @Test
  public void nowAggregateTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "OrderNow1 := SELECT o.customerid as customer, endOfDay(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer, bucket");
    builder.add(
        "OrderNow2 := SELECT endOfDay(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY bucket");
    builder.add(
        "OrderNow3 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer");
    builder.add(
        "OrderAugment := SELECT o.id, o.\"time\", c.order_count FROM Orders o JOIN OrderNow3 c ON o.customerid = c.customer");
    plan(builder.toString());
    validateQueryTable("ordernow1", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 2,
        TimestampTest.fixed(1), PullupTest.builder().hasNowFilter(true).build());
    validateQueryTable("ordernow2", TableType.STREAM, ExecutionEngine.Type.STREAM, 2, 1,
        TimestampTest.fixed(0), PullupTest.builder().hasNowFilter(true).build());
    validateQueryTable("ordernow3", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("orderaugment", TableType.STREAM, ExecutionEngine.Type.STREAM, 4, 1,
        TimestampTest.fixed(2));
  }

  @Test
  public void nowAggregateTestWOGroupBY() {
    ScriptBuilder builder = imports();
    builder.add("RecentTotal := SELECT sum(e.unit_price * e.quantity) AS total, sum(e.quantity) AS quantity "
        + "FROM Orders o JOIN o.entries e WHERE o.time > now() - INTERVAL 7 DAY;");
    plan(builder.getScript());
    validateQueryTable("recenttotal", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 4, 1,
        TimestampTest.fixed(3), PullupTest.builder().hasTopN(true).build());

  }

  @Test
  public void streamStateAggregateTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    builder.add(
        "agg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");
    plan(builder.toString());
    validateQueryTable("agg1", TableType.STATE, ExecutionEngine.Type.DATABASE, 2, 1,
        TimestampTest.NONE);
  }

  /*
  ===== FILTER TESTS ======
   */

  @Test
  public void nowFilterTest() {
    ScriptBuilder builder = imports();
    builder.add("OrderFilter := SELECT * FROM Orders WHERE \"time\" > now() - INTERVAL 1 YEAR;\n");

    builder.add(
        "OrderAgg1 := SELECT o.customerid as customer, endOfsecond(o.\"time\") as bucket, COUNT(o.id) as order_count FROM OrderFilter o GROUP BY customer, bucket;\n");
//    //The following should be equivalent
    builder.add(
        "OrderAgg2 := SELECT o.customerid as customer, endOfsecond(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o WHERE o.\"time\" > now() - INTERVAL 1 YEAR GROUP BY customer, bucket;\n");
    plan(builder.toString());
    validateQueryTable("orderfilter", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 1,
        TimestampTest.fixed(4), new PullupTest(true, false));
    validateQueryTable("orderagg1", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 2,
        TimestampTest.fixed(1), new PullupTest(true, false));
    validateQueryTable("orderagg2", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 2,
        TimestampTest.fixed(1), new PullupTest(true, false));
  }

  /*
  ===== TOPN TESTS ======
   */

  @Test
  public void topNTest() {
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY \"_ingest_time\" DESC;");
    builder.add(
        "Customer.recentOrders := SELECT o.id, o.time FROM Orders o WHERE @.customerid = o.customerid ORDER BY o.\"time\" DESC LIMIT 10;");
    plan(builder.toString());
    validateQueryTable("customer", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.fixed(2),
        PullupTest.builder().hasTopN(true).build()); //customerid got moved to the front
    validateQueryTable("recentOrders", TableType.STATE, ExecutionEngine.Type.STREAM, 4, 2,
        TimestampTest.fixed(3), PullupTest.builder().hasTopN(true).build());
  }

  @Test
  public void selectDistinctTest() {
    ScriptBuilder builder = imports();
    builder.add("CustomerId := SELECT DISTINCT customerid FROM Customer;");
    builder.add(
        "CustomerOrders := SELECT o.id, c.customerid FROM CustomerId c JOIN Orders o ON o.customerid = c.customerid");
    plan(builder.toString());
    validateQueryTable("customerid", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 2, 1,
        TimestampTest.fixed(1), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("customerorders", TableType.STREAM, ExecutionEngine.Type.STREAM, 4, 1,
        TimestampTest.fixed(3));
  }

  @Test
  public void selectDistinctNestedTest() {
    ScriptBuilder builder = imports();
    builder.add("ProductId := SELECT DISTINCT productid FROM Orders.entries;");
    builder.add(
        "ProductOrders := SELECT o.id, p.productid FROM ProductId p JOIN Orders.entries e ON e.productid = p.productid JOIN e.parent o");
    builder.add(
        "ProductId.suborders := SELECT o.id as orderid, COUNT(1) AS numOrders, MAX(o.time) AS lastOrder FROM @ "
            + "JOIN Orders.entries e ON e.productid = @.productid JOIN e.parent o GROUP BY orderid ORDER BY numOrders DESC");
    plan(builder.toString());
    validateQueryTable("productid", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 2, 1,
        TimestampTest.fixed(1), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("productorders", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 2,
        TimestampTest.fixed(4));
    validateQueryTable("suborders", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 5, 2,
        TimestampTest.fixed(4), PullupTest.builder().hasTopN(true).hasSort(true).build());
  }
  //

  @Test
  public void partitionSelectDistinctTest() {
    //TODO: add distinctAgg test back in once we keep parent state
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;");
    builder.add(
        "Customer.distinctOrders := SELECT DISTINCT o.id FROM Orders o WHERE @.customerid = o.customerid ORDER BY o.id DESC LIMIT 10;");
    builder.add(
        "Customer.distinctOrdersTime := SELECT DISTINCT o.id, o.time FROM Orders o WHERE @.customerid = o.customerid ORDER BY o.time DESC LIMIT 10;");
//    builder.add("Customer.distinctAgg := SELECT COUNT(d.id) FROM @.distinctOrders d");
    plan(builder.toString());
    validateQueryTable("customer", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.fixed(2),
        PullupTest.builder().hasTopN(true).build()); //customerid got moved to the front
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.fixed(4)); //temporal join fixes timestamp
    validateQueryTable("distinctorders", TableType.STATE, ExecutionEngine.Type.STREAM, 3, 2,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("distinctorderstime", TableType.STATE, ExecutionEngine.Type.STREAM, 3, 3,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
//    validateQueryTable("distinctAgg", TableType.TEMPORAL_STATE,3, 2, TimestampTest.fixed(2));
  }

  @Test
  public void distinctTest() {
    ScriptBuilder builder = imports();
    builder.add("Orders := DISTINCT Orders ON id ORDER BY \"time\" DESC");
    plan(builder.toString());
    validateQueryTable("orders", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 6, 1,
        TimestampTest.fixed(4)); //pullup is inlined because nested
  }

  /*
  ===== SET TESTS ======
   */

  @Test
  public void testUnionWithTimestamp() {
    ScriptBuilder builder = imports();
    builder.add("CombinedStream := (SELECT o.customerid, o.\"time\" AS rowtime FROM Orders o)" +
        " UNION ALL " +
        "(SELECT c.customerid, c.\"_ingest_time\" AS rowtime FROM Customer c);");
    builder.add(
        "StreamCount := SELECT endOfDay(rowtime) as day, COUNT(1) as num FROM CombinedStream GROUP BY day");
    plan(builder.toString());
    validateQueryTable("combinedstream", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.fixed(2));
    validateQueryTable("streamcount", TableType.STREAM, ExecutionEngine.Type.STREAM, 2, 1,
        TimestampTest.fixed(0));
  }

  @Test
  public void testUnionWithoutTimestamp() {
    ScriptBuilder builder = imports();
    builder.add("CombinedStream := (SELECT o.customerid FROM Orders o)" +
        " UNION ALL " +
        "(SELECT c.customerid FROM Customer c);");
    plan(builder.toString());
    validateQueryTable("combinedstream", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.fixed(2));
  }

  /*
  ===== STREAM TESTS ======
   */

  @Test
  public void convert2StreamTest() {
    ScriptBuilder builder = imports();
    builder.add("Product.updateTime := _ingest_time - INTERVAL 1 YEAR");
    builder.add("Product := DISTINCT Product ON productid ORDER BY updateTime DESC");
    builder.add(
        "ProductCount := SELECT p.productid, p.name, SUM(e.quantity) as quantity FROM Orders.entries e JOIN Product p on e.productid = p.productid GROUP BY p.productid, p.name");
    builder.add(
        "CountStream := STREAM ON ADD AS SELECT productid, name, quantity FROM ProductCount WHERE quantity > 1");
    builder.add("ProductCount2 := DISTINCT CountStream ON productid ORDER BY _source_time DESC");
    plan(builder.toString());
    validateQueryTable("productcount", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 4, 2,
        TimestampTest.fixed(3), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("countstream", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 1,
        TimestampTest.fixed(1));
    validateQueryTable("productcount2", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 5, 1,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
  }

  @Test
  public void exportStreamTest() {
    ScriptBuilder builder = imports();
    builder.add(
        "CountStream := STREAM ON ADD AS SELECT customerid, COUNT(1) as num_orders FROM Orders GROUP BY customerid"); //TODO: change to update
    builder.add("EXPORT CountStream TO print.CountStream");
    builder.add("EXPORT CountStream TO output.CountStream");
    plan(builder.toString());
    validateQueryTable("countstream", TableType.STREAM, ExecutionEngine.Type.STREAM, 4, 1,
        TimestampTest.fixed(1));
  }


  /*
  ===== HINT TESTS ======
   */

  @Test
  public void testExecutionTypeHint() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Customer");
    builder.add(
        "CustomerAgg1 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");
    builder.add(
        "/*+ EXEC_DB */ CustomerAgg2 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");
    builder.add(
        "/*+ EXEC_STREAM */ CustomerAgg3 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");

    plan(builder.getScript());
    validateQueryTable("customeragg1", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("customeragg2", TableType.TEMPORAL_STATE, ExecutionEngine.Type.DATABASE, 3,
        1, TimestampTest.fixed(2));
    validateQueryTable("customeragg3", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 3, 1,
        TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
  }

  private ScriptBuilder imports() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.append("IMPORT ecommerce-data.Customer");
    builder.append("IMPORT ecommerce-data.Orders");
    builder.append("IMPORT ecommerce-data.Product");
    builder.append("IMPORT time.*");
    return builder;
  }

  //
//  private void validateQueryTable(String name, TableType tableType, ExecutionEngine.Type execType,
//      int numCols, int numPrimaryKeys) {
//    validateQueryTable(name, tableType, execType, numCols, numPrimaryKeys, TimestampTest.NO_TEST,
//        PullupTest.EMPTY);
//  }

  private void validateQueryTable(String name, TableType tableType, ExecutionEngine.Type execType,
      int numCols, int numPrimaryKeys, TimestampTest timestampTest) {
    validateQueryTable(name, tableType, execType, numCols, numPrimaryKeys, timestampTest,
        PullupTest.EMPTY);
  }

  private void validateQueryTable(String tableName, TableType tableType,
      ExecutionEngine.Type execType,
      int numCols, int numPrimaryKeys, TimestampTest timestampTest,
      PullupTest pullupTest) {
    CalciteSchema relSchema = this.schema;
    QueryRelationalTable table = getLatestTable(relSchema, tableName,
        QueryRelationalTable.class).get();
    snapshot.addContent(TestRelWriter.explain(table.getRelNode()), tableName, "lp");
    assertEquals(tableType, table.getType(), "table type");
    assertEquals(table.getExecution().getEngine().getType(), execType, "execution type");
    assertEquals(numPrimaryKeys, table.getNumPrimaryKeys(), "primary key size");
    assertEquals(numCols, table.getRowType().getFieldCount(), "field count");
    timestampTest.test(table.getTimestamp());
    pullupTest.test(table.getPullups());
  }

  public static <T extends AbstractRelationalTable> Optional<T> getLatestTable(
      CalciteSchema relSchema, String tableName, Class<T> tableClass) {
    String normalizedName = Name.system(tableName).getCanonical();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    return relSchema.getTableNames().stream().filter(s -> s.indexOf(Name.NAME_DELIMITER) != -1)
        .filter(s -> s.substring(0, s.indexOf(Name.NAME_DELIMITER)).equals(normalizedName))
        .filter(s ->
            tableClass.isInstance(relSchema.getTable(s, true).getTable()))
        //Get most recently added table
        .sorted((a, b) -> -Integer.compare(CalciteTableFactory.getTableOrdinal(a),
            CalciteTableFactory.getTableOrdinal(b)))
        .findFirst().map(s -> tableClass.cast(relSchema.getTable(s, true).getTable()));
  }

  @Value
  @Builder
  @AllArgsConstructor
  public static class PullupTest {

    public static final PullupTest EMPTY = new PullupTest(false, false, false);

    boolean hasNowFilter;
    boolean hasTopN;
    boolean hasSort;

    public PullupTest(boolean hasNowFilter, boolean hasTopN) {
      this(hasNowFilter, hasTopN, false);
    }

    public void test(PullupOperator.Container pullups) {
      assertEquals(hasNowFilter, !pullups.getNowFilter().isEmpty(), "now filter");
      assertEquals(hasTopN, !pullups.getTopN().isEmpty(), "topN");
      assertEquals(hasSort, !pullups.getSort().isEmpty(), "sort");
    }

    public static PullupTest hasTopN() {
      return PullupTest.builder().hasTopN(true).build();
    }

  }

  @Value
  public static class TimestampTest {

    public static final TimestampTest NONE = new TimestampTest(Type.NONE, new Integer[0]);
    public static final TimestampTest NO_TEST = new TimestampTest(Type.NO_TEST, new Integer[0]);

    enum Type {NO_TEST, NONE, FIXED, CANDIDATES, BEST}

    final Type type;
    final Integer[] candidates;

    public static TimestampTest best(Integer best) {
      return new TimestampTest(Type.BEST, new Integer[]{best});
    }

    public static TimestampTest fixed(Integer fixed) {
      return new TimestampTest(Type.FIXED, new Integer[]{fixed});
    }

    public static TimestampTest candidates(Integer... candidates) {
      return new TimestampTest(Type.CANDIDATES, candidates);
    }

    public void test(TimestampHolder.Base timeHolder) {
      if (type == Type.NO_TEST) {
        return;
      }
      if (type == Type.NONE) {
        assertFalse(timeHolder.hasFixedTimestamp(), "no timestamp");
        assertEquals(0, timeHolder.getCandidates().size(), "candidate size");
      } else if (type == Type.FIXED) {
        assertTrue(timeHolder.hasFixedTimestamp(), "has timestamp");
        assertEquals(1, candidates.length, "candidate size");
        assertEquals(candidates[0], timeHolder.getTimestampCandidate().getIndex(),
            "timestamp index");
      } else if (type == Type.BEST) {
        assertFalse(timeHolder.hasFixedTimestamp(), "no timestamp");
        assertEquals(1, candidates.length, "candidate size");
        assertEquals(candidates[0], timeHolder.getBestCandidate().getIndex(), "best candidate");
      } else if (type == Type.CANDIDATES) {
        assertFalse(timeHolder.hasFixedTimestamp(), "no timestamp");
        assertEquals(Sets.newHashSet(candidates),
            timeHolder.getCandidates().stream().map(TimestampHolder.Candidate::getIndex)
                .collect(Collectors.toSet()), "candidate set");
      }
    }

  }

}
