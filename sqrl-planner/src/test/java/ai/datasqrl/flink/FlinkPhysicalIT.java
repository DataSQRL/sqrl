package com.datasqrl.flink;

import com.datasqrl.AbstractPhysicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Retail;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
    initialize(IntegrationTestSettings.getFlinkWithDB(), example.getRootPackageDirectory());
    this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
    if (!Files.isDirectory(exportPath)) Files.createDirectory(exportPath);
  }

  @AfterEach
  @SneakyThrows
  public void cleanupDirectory() {
    if (Files.isDirectory(exportPath)) FileUtils.deleteDirectory(exportPath.toFile());
  }

  @Test
  public void tableImportTest() {
    String script = example.getImports().toString();
    validateTables(script, "customer","product","orders","entries");
  }

  @Test
  public void tableColumnDefinitionTest() {
    ScriptBuilder builder = example.getImports();

    builder.add("EntryPrice := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e"); //This is line 4 in the script

    builder.add("Customer.timestamp := EPOCH_TO_TIMESTAMP(lastUpdated)");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY timestamp DESC");

    builder.add("Orders.col1 := (id + customerid)/2");
    builder.add("Orders.entries.discount2 := COALESCE(discount,0.0)");

    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid, o.col1, e.discount2 FROM Orders o JOIN o.entries e JOIN Customer c on o.customerid = c.customerid");

    validateTables(builder.getScript(),"entryprice","customer","orders","entries","ordercustomer");
  }

  @Test
  public void nestedAggregationandSelfJoinTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Orders");
    builder.add("IMPORT ecommerce-data.Customer TIMESTAMP EPOCH_TO_TIMESTAMP(lastUpdated) AS updateTime");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add("Orders.total := SELECT SUM(e.quantity * e.unit_price - e.discount) as price, COUNT(e.quantity) as num, SUM(e.discount) as discount FROM _.entries e");
    builder.add("OrdersInline := SELECT o.id, o.customerid, o.\"time\", t.price, t.num FROM Orders o JOIN o.total t");
//    builder.add("Customer.orders_by_day := SELECT o.\"time\", o.price, o.num FROM _ JOIN OrdersInline o ON o.customerid = _.customerid");
    builder.add("Customer.orders_by_hour := SELECT round_to_hour(o.\"time\") as hour, SUM(o.price) as total_price, SUM(o.num) as total_num FROM _ JOIN OrdersInline o ON o.customerid = _.customerid GROUP BY hour");
    validateTables(builder.getScript(),"customer","orders","ordersinline","orders_by_hour");
  }

  @Test
  public void joinTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Customer TIMESTAMP epoch_to_timestamp(lastUpdated) as updateTime"); //we fake that customer updates happen before orders
    builder.add("IMPORT ecommerce-data.Orders TIMESTAMP \"time\" AS rowtime");

    //Normal join
    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid");
    //Interval join
    builder.add("OrderCustomerInterval := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "+
            "AND o.rowtime >= c.updateTime AND o.rowtime <= c.updateTime + INTERVAL 1 YEAR");
    //Temporal join
    builder.add("CustomerDedup := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add("OrderCustomerDedup := SELECT o.id, c.name, o.customerid FROM Orders o JOIN CustomerDedup c on o.customerid = c.customerid");

    validateTables(builder.getScript(),"ordercustomer","ordercustomerinterval","customerdedup","ordercustomerdedup");
  }

  @Test
  public void aggregateTest() {
    ScriptBuilder builder = example.getImports();
    //temporal state
    builder.append("OrderAgg1 := SELECT o.customerid as customer, round_to_hour(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    builder.append("OrderAgg2 := SELECT COUNT(o.id) as order_count FROM Orders o");
    //time window
    builder.append("Ordertime1 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    //now() and sliding window
    builder.append("OrderNow1 := SELECT o.customerid as customer, round_to_hour(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer, bucket");
    builder.append("OrderNow2 := SELECT round_to_hour(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY bucket");
    builder.append("OrderNow3 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer");
    builder.append("OrderAugment := SELECT o.id, o.\"time\", c.order_count FROM Orders o JOIN OrderNow3 c ON o.customerid = c.customer"); //will be empty because OrderNow3 has a timestamp greater than Order
    //state
    builder.append("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    builder.append("agg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");

    validateTables(builder.getScript(),Set.of("ordernow3"),"orderagg1", "orderagg2","ordertime1","ordernow1",
            "ordernow2","ordernow3","orderaugment","ordercustomer","agg1");
  }

  @Test
  public void filterTest() {
    ScriptBuilder builder = example.getImports();

    builder.add("HistoricOrders := SELECT * FROM Orders WHERE \"time\" >= now() - INTERVAL 5 YEAR");
    builder.add("RecentOrders := SELECT * FROM Orders WHERE \"time\" >= now() - INTERVAL 1 SECOND");

    validateTables(builder.getScript(), "historicorders", "recentorders");
  }

  @Test
  public void topNTest() {
    ScriptBuilder builder = example.getImports();

    builder.add("Customer.updateTime := epoch_to_timestamp(lastUpdated)");
    builder.add("CustomerDistinct := DISTINCT Customer ON customerid ORDER BY updateTime DESC;");
    builder.add("CustomerDistinct.recentOrders := SELECT o.id, o.time FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.\"time\" DESC LIMIT 10;");

    builder.add("CustomerId := SELECT DISTINCT customerid FROM Customer;");
    builder.add("CustomerOrders := SELECT o.id, c.customerid FROM CustomerId c JOIN Orders o ON o.customerid = c.customerid");

    builder.add("CustomerDistinct.distinctOrders := SELECT DISTINCT o.id FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.id DESC LIMIT 10;");
    builder.add("CustomerDistinct.distinctOrdersTime := SELECT DISTINCT o.id, o.time FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.time DESC LIMIT 10;");

    builder.add("Orders := DISTINCT Orders ON id ORDER BY \"time\" DESC");

    validateTables(builder.getScript(),"customerdistinct","customerid","customerorders","distinctorders","distinctorderstime","orders","entries");
  }

  @Test
  public void setTest() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Customer TIMESTAMP epoch_to_timestamp(lastUpdated) as updateTime"); //we fake that customer updates happen before orders
    builder.add("IMPORT ecommerce-data.Orders");

    builder.add("CombinedStream := (SELECT o.customerid, o.\"time\" AS rowtime FROM Orders o)" +
            " UNION ALL " +
            "(SELECT c.customerid, c.updateTime AS rowtime FROM Customer c);");
    builder.add("StreamCount := SELECT round_to_hour(rowtime) as hour, COUNT(1) as num FROM CombinedStream GROUP BY hour");
    validateTables(builder.getScript(), "combinedstream","streamcount");
  }

  @Test
  public void streamTest() {
    ScriptBuilder builder = example.getImports();
    builder.add("Customer.updateTime := epoch_to_timestamp(lastUpdated)");
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY updateTime DESC");
    builder.add("CustomerCount := SELECT c.customerid, c.name, SUM(e.quantity) as quantity FROM Orders o JOIN o.entries e JOIN Customer c on o.customerid = c.customerid GROUP BY c.customerid, c.name");
    builder.add("CountStream := STREAM ON ADD AS SELECT customerid, name, quantity FROM CustomerCount WHERE quantity > 1");
    builder.add("CustomerCount2 := DISTINCT CountStream ON customerid ORDER BY _ingest_time DESC");
    builder.add("EXPORT CountStream TO print.CountStream");
    builder.add("EXPORT CountStream TO output.CountStream");
    validateTables(builder.getScript(), "customercount","countstream","customercount2");
  }
}
