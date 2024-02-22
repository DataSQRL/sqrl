package com.datasqrl.plan.local.analyze;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.IntegrationTestSettings;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.global.SqrlDAGExporter;
import com.datasqrl.plan.table.TableType;
import com.datasqrl.util.ScriptBuilder;
import com.datasqrl.util.SnapshotTest;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DAGPlannerTest extends AbstractLogicalSQRLIT {

    protected SnapshotTest.Snapshot snapshot;

    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        initialize(IntegrationTestSettings.getInMemory(), null, Optional.empty());
        this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    }

    @AfterEach
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        snapshot.createOrValidate();
    }

    /* ==== IMPORT ==== */

    @Test
    public void importTest() {
        ScriptBuilder s = imports(false);
        validateTables(s, "customer", "orders", "product");
    }

    @Test
    public void importWithNaturalTimestampTest() {
        ScriptBuilder s = imports(true);
        validateTables(s, "customer", "orders", "product");
    }

    @Test
    public void timestampColumnDefinitionWithPropagation() {
        ScriptBuilder script = ScriptBuilder.of("IMPORT time.*",
                        "IMPORT ecommerce-data.Customer TIMESTAMP epochToTimestamp(lastUpdated) AS timestamp",
                        "CustomerCopy := SELECT endOfMonth(endOfMonth(timestamp)) as month, timestamp  FROM Customer");
        validateTables(script, "customer", "customercopy");
    }

    @Test
    public void addingSimpleColumns() {
        ScriptBuilder script = imports(true)
                .add("Orders.col1 := (id + customerid)/2")
                .add("OrderEntry := SELECT o.col1, o.time, e.productid, e.discount, o._ingest_time FROM Orders o JOIN o.entries e");
        validateTables(script, "orders", "orderentry");
    }

    /* ==== NESTED === */
    @Test
    public void nestedAggregationAndSelfJoinTest() {
        ScriptBuilder s = ScriptBuilder.of(
                "IMPORT ecommerce-data.Orders TIMESTAMP time",
                        "IMPORT ecommerce-data.Customer TIMESTAMP _ingest_time",
                        "IMPORT time.*",
                        "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC")
                .add("Orders2", "SELECT o2._uuid FROM Orders o2 INNER JOIN (SELECT _uuid FROM Orders) AS o ON o._uuid = o2._uuid")
                .add("EntryCount", "SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e")

                .add("Orders.total","SELECT SUM(e.quantity * e.unit_price - e.discount) as price, COUNT(e.quantity) as num, SUM(e.discount) as discount FROM @.entries e")
                .add("OrdersInline","SELECT o.id, o.customerid, o.time, t.price, t.num FROM Orders o JOIN o.total t")
                .add("Customer.orders_by_day","SELECT endOfDay(o.time) as day, SUM(o.price) as total_price, SUM(o.num) as total_num FROM @ JOIN OrdersInline o ON o.customerid = @.customerid GROUP BY day");
        validateTables(s);
    }

    /* ==== JOIN === */

    @Test
    public void joinTimestampPropagationTest() {
        ScriptBuilder builder = imports(false);
        builder.add("OrderCustomer1","SELECT o.id, c.name FROM Orders o JOIN Customer c on o.customerid = c.customerid");
        builder.add("OrderCustomer2","SELECT o.id, c.name, GREATEST(o._ingest_time, c._ingest_time) AS timestamp FROM Orders o JOIN Customer c on o.customerid = c.customerid");
        builder.add("OrderCustomer3","SELECT o.id, c.name, p.name FROM Orders o JOIN Customer c on o.customerid = c.customerid JOIN Product p ON p.productid = c.customerid");
        validateTables(builder);
    }

    @Test
    public void tableStreamJoinTest() {
        ScriptBuilder script = imports(false);
        script.add(
                "OrderCustomerLeft","SELECT coalesce(c._uuid, '') as cuuid, o.id, c.name, o.customerid  FROM Orders o LEFT JOIN Customer c on o.customerid = c.customerid;");
        script.add(
                "OrderCustomer","SELECT o._uuid, o.id, c.name, o.customerid FROM Orders o INNER JOIN Customer c on o.customerid = c.customerid;");
        script.add(
                "OrderCustomerLeftExcluded","SELECT o._uuid, o.id, o.customerid  FROM Orders o LEFT JOIN Customer c on o.customerid = c.customerid WHERE c._uuid IS NULL;");
        script.add(
                "OrderCustomerRight","SELECT coalesce(o._uuid, '') as ouuid, o.id, c.name, o.customerid  FROM Orders o RIGHT JOIN Customer c on o.customerid = c.customerid;");
        script.add(
                "OrderCustomerRightExcluded","SELECT c._uuid, c.customerid, c.name  FROM Orders o RIGHT JOIN Customer c on o.customerid = c.customerid WHERE o._uuid IS NULL;");
        script.add(
                "OrderCustomerConstant","SELECT o._uuid, o.id, c.name, o.customerid FROM Orders o INNER JOIN Customer c ON o.customerid = c.customerid AND c.name = 'Robert' AND o.id > 5;");
        validateTables(script);
    }

    @Test
    public void tableStateJoinTest() {
        ScriptBuilder script = imports(false);
        script.add("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC");
        script.add("Orders := DISTINCT Orders ON id ORDER BY _ingest_time DESC");
        script.add(
                "OrderCustomerLeft","SELECT c._uuid, o.id, c.name, o.customerid  FROM Orders o LEFT JOIN Customer c on o.customerid = c.customerid;");
        script.add(
                "OrderCustomer","SELECT o._uuid, o.id, c.name, o.customerid FROM Orders o INNER JOIN Customer c on o.customerid = c.customerid;");
        script.add(
                "OrderCustomerLeftExcluded","SELECT o._uuid, o.id, o.customerid  FROM Orders o LEFT JOIN Customer c on o.customerid = c.customerid WHERE c._uuid IS NULL;");
        script.add(
                "OrderCustomerRight","SELECT coalesce(o.id, 0) as ouuid, o.id, c.name, o.customerid  FROM Orders o RIGHT JOIN Customer c on o.customerid = c.customerid;");
        script.add(
                "OrderCustomerRightExcluded","SELECT c._uuid, c.customerid, c.name  FROM Orders o RIGHT JOIN Customer c on o.customerid = c.customerid WHERE o.id IS NULL;");
        script.add(
                "OrderCustomerConstant","SELECT o._uuid, o.id, c.name, o.customerid FROM Orders o INNER JOIN Customer c ON o.customerid = c.customerid AND c.name = 'Robert' AND o.id > 5;");
        validateTables(script);

    }

    @Test
    public void tableIntervalJoinTest() {
        ScriptBuilder builder = imports(true);
        builder.add(
                "OrderCustomer","SELECT o.id, c.name, o.customerid FROM Orders o INTERVAL JOIN Customer c on o.customerid = c.customerid " +
                        "AND o.time > c.timestamp AND o.time <= c.timestamp + INTERVAL 31 DAYS;");
        builder.add(
                "OrderCustomer2","SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid " +
                        "AND o.time > c.timestamp AND o.time <= c.timestamp + INTERVAL 31 DAYS;");
        builder.add(
                "OrderCustomer3","SELECT o.id, c.name, o.customerid FROM Orders o INTERVAL JOIN Customer c on o.customerid = c.customerid " +
                        "AND o.time >= c.timestamp + INTERVAL 2 DAYS AND o.time <= c.timestamp + INTERVAL 31 DAYS;");
        validateTables(builder, "ordercustomer", "ordercustomer2", "ordercustomer3");
    }

    @Test
    public void joinTypesTest() {
        ScriptBuilder builder = imports(true);
        builder.add("Customer := DISTINCT Customer ON customerid ORDER BY timestamp DESC");
        builder.add("OrderCustomer","SELECT o.id, c.name FROM Orders o LEFT JOIN Customer c ON o.customerid = c.customerid");
        builder.add("OrderCustomer2","SELECT o.id, c.name FROM Orders o LEFT TEMPORAL JOIN Customer c ON o.customerid = c.customerid");
        builder.add("OrderCustomer3","SELECT o.id, c.name FROM Customer c RIGHT JOIN Orders o ON o.customerid = c.customerid");
        builder.add("OrderCustomer4","SELECT o.id, c.name FROM Orders o LEFT OUTER JOIN Customer c ON o.customerid = c.customerid");
        validateTables(builder);
    }

    @Test
    @Disabled("de-corelation issue")
    public void tableTemporalJoinWithTimeFilterTest() {
        ScriptBuilder builder = imports(true);
        builder.add("Customer := DISTINCT Customer ON customerid ORDER BY timestamp DESC");
        builder.add("Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC");
        builder.add("Customer.orders := JOIN Orders ON Orders.customerid = @.customerid");
        builder.add("Orders.entries.product := JOIN Product ON Product.productid = @.productid");
        //The last join to Product does not get de-correlated
        builder.add("Customer.totals","SELECT p.category as category, sum(e.quantity) as num " +
                "FROM @.orders o JOIN o.entries e JOIN e.product p WHERE o.time >= now() - INTERVAL 1 DAY GROUP BY category");
        validateTables(builder);
    }

      /*
  ===== AGGREGATE ======
   */

    @Test
    public void streamAggregateTest() {
        ScriptBuilder builder = imports(true);
        builder.add(
                "OrderAgg1","SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer");
        builder.add("OrderAgg2","SELECT COUNT(o.id) as order_count FROM Orders o;");
        builder.add(
                "Ordertime1","SELECT o.customerid as customer, endOfsecond(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
        builder.add(
                "Ordertime2","SELECT o.customerid as customer, endOfMinute(o.time, 1, 15) as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
        builder.add(
                "Ordertime3","SELECT o.customerid as customer, endOfHour(o.time, 5, 30) as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
        validateTables(builder);
    }

    @Test
    public void nowAggregateTest() {
        ScriptBuilder builder = imports(true);
        builder.add(
                "OrderNow1","SELECT o.customerid as customer, endOfDay(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.time > NOW() - INTERVAL 8 DAY) GROUP BY customer, bucket");
        builder.add(
                "OrderNow2","SELECT endOfDay(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.time > NOW() - INTERVAL 8 DAY) GROUP BY bucket");
        builder.add(
                "OrderNow3","SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o  WHERE (o.time > NOW() - INTERVAL 8 DAY) GROUP BY customer");
        builder.add(
                "OrderAugment","SELECT o.id, o.time, c.order_count FROM Orders o JOIN OrderNow3 c ON o.customerid = c.customer");
        builder.add("RecentTotal","SELECT sum(e.unit_price * e.quantity) AS total, sum(e.quantity) AS quantity "
                + "FROM Orders o JOIN o.entries e WHERE o.time > now() - INTERVAL 7 DAY;");
        validateTables(builder);
    }

    @Test
    public void streamStateAggregateTest() {
        ScriptBuilder builder = imports(true);
        builder.add(
                "OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
        builder.add(
                "agg1","SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");
        validateTables(builder);
    }

    @Test
    public void aggregateWithMaxTimestamp() {
        ScriptBuilder builder = imports(true);
        builder.add("OrdersState := DISTINCT Orders ON id ORDER BY time DESC");
        builder.add("OrderAgg1","SELECT customerid, COUNT(1) as count FROM Orders GROUP BY customerid");
        builder.add("OrderAgg2","SELECT customerid, MAX(time) as timestamp, COUNT(1) as count FROM Orders GROUP BY customerid");
        builder.add("OrderAgg3","SELECT customerid, COUNT(1) as count FROM OrdersState GROUP BY customerid");
        builder.add("OrderAgg4","SELECT customerid, MAX(time) as timestamp, COUNT(1) as count FROM OrdersState GROUP BY customerid");
        validateTables(builder);
    }

  /*
  ===== FILTER TESTS ======
   */

    @Test
    public void nowFilterTest() {
        ScriptBuilder builder = imports(true);
        builder.add("OrderFilter","SELECT * FROM Orders WHERE time > now() - INTERVAL 1 DAY;\n");

        builder.add(
                "OrderAgg1","SELECT o.customerid as customer, endOfsecond(o.time) as bucket, COUNT(o.id) as order_count FROM OrderFilter o GROUP BY customer, bucket;\n");
//    //The following should be equivalent
        builder.add(
                "OrderAgg2","SELECT o.customerid as customer, endOfsecond(o.time) as bucket, COUNT(o.id) as order_count FROM Orders o WHERE o.time > now() - INTERVAL 1 DAY GROUP BY customer, bucket;\n");
        validateTables(builder);
    }

  /*
  ===== TOPN TESTS ======
   */

    @Test
    public void topNTest() {
        ScriptBuilder builder = imports(true);
        builder.add("Customer := DISTINCT Customer ON customerid ORDER BY timestamp DESC;");
        //TODO: @Daniel - the topN hint is coming through wrong - it's on index 2 instead of 0.
        builder.add(
                "Customer.recentOrders","SELECT o.id, o.time FROM @ TEMPORAL JOIN Orders o WHERE @.customerid = o.customerid ORDER BY o.time DESC LIMIT 10;");
        validateTables(builder);
    }

    @Test
    public void selectDistinctTest() {
        ScriptBuilder builder = imports(true);
        builder.add("CustomerId","SELECT DISTINCT customerid FROM Customer;");
        builder.add(
                "CustomerOrders","SELECT o.id, c.customerid FROM CustomerId c JOIN Orders o ON o.customerid = c.customerid");
        validateTables(builder);
    }

    @Test
    public void selectDistinctNestedTest() {
        ScriptBuilder builder = imports(true);
        builder.add("ProductId","SELECT DISTINCT productid FROM Orders.entries;");
        builder.add(
                "ProductOrders","SELECT o.id, p.productid FROM ProductId p JOIN Orders o JOIN o.entries e ON e.productid = p.productid");
        builder.add(
                "ProductId.suborders","SELECT o.id as orderid, COUNT(1) AS numOrders, MAX(o.time) AS lastOrder FROM @ "
                        + "JOIN Orders o JOIN o.entries e ON e.productid = @.productid GROUP BY orderid ORDER BY numOrders DESC");
        validateTables(builder);
    }

      /*
  ===== SET TESTS ======
   */

    @Test
    public void unionWithTimestamp() {
        ScriptBuilder builder = imports(true);
        builder.add("CombinedStream","SELECT o.customerid, o.time AS rowtime FROM Orders o" +
                " UNION ALL " +
                "SELECT c.customerid, c.timestamp AS rowtime FROM Customer c;");
        builder.add(
                "StreamCount","SELECT endOfDay(rowtime) as day, COUNT(1) as num FROM CombinedStream GROUP BY day");
        validateTables(builder);
    }

    @Test
    public void unionWithoutTimestamp() {
        ScriptBuilder builder = imports(false);
        builder.add("CombinedStream","SELECT o.customerid FROM Orders o" +
                " UNION ALL " +
                "SELECT c.customerid FROM Customer c;");
        validateTables(builder);
    }

  /*
  ===== EXPORT TESTS ======
   */


    @Test
    public void exportStreamTest() {
        ScriptBuilder builder = imports(true);
        builder.add(
                "JoinStream := SELECT o.id, c.name FROM Orders o JOIN Customer c on c.customerid = o.customerid");
        builder.add("EXPORT JoinStream TO print.CountStream");
        validateTables(builder);
    }

  /*
  ===== TABLE FUNCTIONS TESTS ======
  */

    @Test
    @Disabled("figure out how to generate queries for table functions")
    public void tableFunctionsBasic() {
        ScriptBuilder builder = imports(true);
        builder.add(
                "OrdersIDRange(@idLower: Int)","SELECT * FROM Orders WHERE id > @idLower");
        builder.add(
                "Orders2(@idLower: Int)","SELECT id, id - @idLower AS delta, time FROM Orders WHERE id > @idLower");
        validateTables(builder);
//        validateAccessTableFunction("ordersidrange", "orders", ExecutionEngine.Type.DATABASE);
//        validateTableFunction("orders2", TableType.STREAM, ExecutionEngine.Type.DATABASE, 4, 1,
//                ResolveTest.TimestampTest.fixed(3), ResolveTest.PullupTest.EMPTY);
    }


  /*
  ===== HINT TESTS ======
   */

    @Test
    public void testExecutionTypeHint() {
        ScriptBuilder builder = imports(false);
        builder.add(
                "CustomerAgg1","SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");
        builder.add(
                "/*+ EXEC(database) */ CustomerAgg2 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");
        builder.add(
                "/*+ EXEC(streams) */ CustomerAgg3 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");

        validateTables(builder,"customeragg2","customeragg3");
    }



    /* ======== UTILITY METHODS ======== */

    private void validateTables(ScriptBuilder script, String... queryTableNames) {
        List<String> tableNames = script.getTables();
        tableNames.addAll(Arrays.asList(queryTableNames));
        SqrlDAG dag = super.planDAG(script.getScript(), tableNames);
        SqrlDAGExporter exporter = SqrlDAGExporter.builder()
                .includeQueries(false)
                .includeImports(false)
                .withHints(true)
                .build();
        //Need to sort so that the order is deterministic
        snapshot.addContent(exporter.export(dag).stream().sorted().map(SqrlDAGExporter.Node::toString)
                .collect(Collectors.joining("\n")));
    }


    public static ScriptBuilder imports(boolean useNaturalTimestamp) {
        ScriptBuilder builder = new ScriptBuilder();
        builder.append("IMPORT time.*");
        builder.append("IMPORT ecommerce-data.Customer  TIMESTAMP " + (useNaturalTimestamp? "epochToTimestamp(lastUpdated) AS timestamp" : "_ingest_time"));
        builder.append("IMPORT ecommerce-data.Orders TIMESTAMP " + (useNaturalTimestamp? "time": "_ingest_time"));
        builder.append("IMPORT ecommerce-data.Product TIMESTAMP _ingest_time");
        return builder;
    }

}
