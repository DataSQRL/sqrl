package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.util.ScriptBuilder;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.data.Retail;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.ScriptNode;
import org.apache.commons.compress.utils.Sets;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class ResolveTest extends AbstractLogicalSQRLIT {

  private final Retail example = Retail.INSTANCE;

  private Resolve.Env resolvedDag = null;
  private SnapshotTest.Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
    this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
  }

  @AfterEach
  public void tearDown() {
    super.tearDown();
    snapshot.createOrValidate();
  }

  /*
  ===== IMPORT TESTS ======
   */

  @Test
  public void tableImportTest() {
    process(imports().toString());
    validateQueryTable("customer", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1, TimestampTest.candidates(1));
    validateQueryTable("product", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1, TimestampTest.candidates(1));
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1, TimestampTest.candidates(1,4));
  }

  /*
  ===== TABLE & COLUMN DEFINITION (PROJECT) ======
   */

  @Test
  public void timestampColumnDefinition() {
    String script = ScriptBuilder.of("IMPORT ecommerce-data.Customer",
            "Customer.timestamp := EPOCH_TO_TIMESTAMP(lastUpdated)",
            "Customer.month := ROUND_TO_MONTH(ROUND_TO_MONTH(timestamp))",
            "CustomerCopy := SELECT timestamp, month FROM Customer");
    process(script);
    validateQueryTable("customer", TableType.STREAM, ExecutionEngine.Type.STREAM, 8, 1, TimestampTest.candidates(1,6,7));
    validateQueryTable("customercopy", TableType.STREAM, ExecutionEngine.Type.STREAM, 4, 1, TimestampTest.candidates(1,2,3));
  }

  @Test
  public void timestampExpressionTest() {
    process("IMPORT ecommerce-data.Customer TIMESTAMP EPOCH_TO_TIMESTAMP(lastUpdated) AS timestamp;\n");
    validateQueryTable("customer", TableType.STREAM, ExecutionEngine.Type.STREAM, 7, 1, TimestampTest.fixed(6));
  }

  @Test
  public void timestampDefinitionTest() {
    process("IMPORT ecommerce-data.Orders TIMESTAMP \"time\";\n");
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1, TimestampTest.fixed(4));
  }

  @Test
  public void addingSimpleColumns() {
    String script = ScriptBuilder.of("IMPORT ecommerce-data.Orders",
            "Orders.col1 := (id + customerid)/2",
            "Orders.entries.discount2 := COALESCE(discount,0.0)",
            "OrderEntry := SELECT o.col1, o.\"time\", e.productid, e.discount2 FROM Orders o JOIN o.entries e");
    process(script);
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM, 7, 1, TimestampTest.candidates(1,4));
    validateQueryTable("orderentry", TableType.STREAM, ExecutionEngine.Type.STREAM, 7, 2, TimestampTest.candidates(3,6));
  }

  @Test
  public void selfJoinSubqueryTest() {
    ScriptBuilder builder = imports();
//    builder.append("IMPORT ecommerce-data.Orders;\n");
    builder.append("Orders2 := SELECT o2._uuid FROM Orders o2 "
            + "INNER JOIN (SELECT _uuid FROM Orders) AS o ON o._uuid = o2._uuid;\n");
    process(builder.toString());
    validateQueryTable("orders2", TableType.STREAM, ExecutionEngine.Type.STREAM, 3, 1, TimestampTest.candidates(1,2));
  }

  @Test
  public void tableDefinitionTest() {
    String sqrl = ScriptBuilder.of("IMPORT ecommerce-data.Orders",
          "EntryCount := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;");
    process(sqrl);
    validateQueryTable("entrycount", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 2, TimestampTest.candidates(3,4)); //5 cols = 1 select col + 2 pk cols + 2 timestamp cols
  }

  /*
  ===== NESTED TABLES ======
   */

  @Test
  public void nestedAggregationAndSelfJoinTest() {
    String sqrl = ScriptBuilder.of("IMPORT ecommerce-data.Orders",
            "IMPORT ecommerce-data.Customer",
            "Customer := DISTINCT Customer ON customerid ORDER BY \"_ingest_time\" DESC",
            "Orders.total := SELECT SUM(e.quantity * e.unit_price - e.discount) as price, COUNT(e.quantity) as num, SUM(e.discount) as discount FROM _.entries e",
            "OrdersInline := SELECT o.id, o.customerid, o.\"time\", t.price, t.num FROM Orders o JOIN o.total t",
            "Customer.orders_by_day := SELECT round_to_day(o.\"time\") as day, SUM(o.price) as total_price, SUM(o.num) as total_num FROM _ JOIN OrdersInline o ON o.customerid = _.customerid GROUP BY day");
    process(sqrl);
    validateQueryTable("total", TableType.STREAM, ExecutionEngine.Type.STREAM, 5, 1, TimestampTest.fixed(4));
    validateQueryTable("ordersinline", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 1, TimestampTest.fixed(3));
    validateQueryTable("orders_by_day", TableType.STREAM, ExecutionEngine.Type.STREAM, 4, 2, TimestampTest.fixed(1));
  }


  /*
  ===== JOINS ======
   */

  @Test
  public void tableJoinTest() {
    ScriptBuilder builder = imports();
    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    process(builder.toString());
    validateQueryTable("ordercustomer", TableType.STATE, ExecutionEngine.Type.DATABASE, 5, 2, TimestampTest.NONE); //numCols = 3 selected cols + 2 uuid cols for pk
  }

  @Test
  public void tableIntervalJoinTest() {
    ScriptBuilder builder = imports();
    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o INTERVAL JOIN Customer c on o.customerid = c.customerid "+
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    builder.append("OrderCustomer2 := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "+
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    process(builder.toString());
    validateQueryTable("ordercustomer", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 2, TimestampTest.fixed(5)); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
    validateQueryTable("ordercustomer2", TableType.STREAM, ExecutionEngine.Type.STREAM, 6, 2, TimestampTest.fixed(5)); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
  }

  @Test
  public void tableTemporalJoinTest() {
    ScriptBuilder builder = imports();
    builder.append("CustomerCount := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer");
    builder.append("OrderWithCount := SELECT o.id, c.order_count, o.customerid FROM Orders o TEMPORAL JOIN CustomerCount c on o.customerid = c.customer");
    builder.append("OrderWithCount2 := SELECT o.id, c.order_count, o.customerid FROM CustomerCount c TEMPORAL JOIN Orders o on o.customerid = c.customer");
    process(builder.toString());
    validateQueryTable("orderwithcount", TableType.STREAM, ExecutionEngine.Type.STREAM,5, 1, TimestampTest.fixed(4)); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
    validateQueryTable("orderwithcount2", TableType.STREAM, ExecutionEngine.Type.STREAM,5, 1, TimestampTest.fixed(4)); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
  }

  @Test
  @Disabled
  public void tableTemporalJoinWithTimeFilterTest() {
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY \"_ingest_time\" DESC");
    builder.add("Customer.orders := JOIN Orders ON Orders.customerid = _.customerid");
    builder.add("Orders.entries.product := JOIN Product ON Product.productid = _.productid");
    builder.append("Customer.totals := SELECT p.category as category, sum(e.quantity) as num " +
            "FROM _.orders o JOIN o.entries e JOIN e.product p WHERE o.\"time\" >= now() - INTERVAL 1 YEAR GROUP BY category");
    process(builder.toString());
    validateQueryTable("totals", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,4, 2, TimestampTest.fixed(3));
  }


  /*
  ===== AGGREGATE ======
   */

  @Test
  public void streamAggregateTest() {
    ScriptBuilder builder = imports();
    builder.append("OrderAgg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer");
    builder.append("OrderAgg2 := SELECT COUNT(o.id) as order_count FROM Orders o;");
    process(builder.toString());
    validateQueryTable("orderagg1", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,3, 1, TimestampTest.fixed(2)); //timestamp column is added
    validateQueryTable("orderagg2", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,3, 1, TimestampTest.fixed(2));
  }

  @Test
  public void streamTimeAggregateTest() {
    ScriptBuilder builder = imports();
    builder.append("Ordertime1 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket");
    process(builder.toString());
    validateQueryTable("ordertime1", TableType.STREAM, ExecutionEngine.Type.STREAM,3, 2, TimestampTest.fixed(1));
  }

  @Test
  public void nowAggregateTest() {
    ScriptBuilder builder = imports();
    builder.append("OrderNow1 := SELECT o.customerid as customer, round_to_day(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer, bucket");
    builder.append("OrderNow2 := SELECT round_to_day(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY bucket");
    builder.append("OrderNow3 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o  WHERE (o.\"time\" > NOW() - INTERVAL 8 YEAR) GROUP BY customer");
    builder.append("OrderAugment := SELECT o.id, o.\"time\", c.order_count FROM Orders o JOIN OrderNow3 c ON o.customerid = c.customer");
    process(builder.toString());
    validateQueryTable("ordernow1", TableType.STREAM, ExecutionEngine.Type.STREAM,3, 2, TimestampTest.fixed(1), PullupTest.builder().hasNowFilter(true).build());
    validateQueryTable("ordernow2", TableType.STREAM, ExecutionEngine.Type.STREAM,2, 1, TimestampTest.fixed(0), PullupTest.builder().hasNowFilter(true).build());
    validateQueryTable("ordernow3", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,3, 1, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("orderaugment", TableType.STREAM, ExecutionEngine.Type.STREAM,4, 1, TimestampTest.fixed(2));
  }

  @Test
  public void streamStateAggregateTest() {
    ScriptBuilder builder = imports();
    builder.append("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    builder.append("agg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");
    process(builder.toString());
    validateQueryTable("agg1", TableType.STATE, ExecutionEngine.Type.DATABASE,2, 1, TimestampTest.NONE);
  }

  /*
  ===== FILTER TESTS ======
   */

  @Test
  public void nowFilterTest() {
    ScriptBuilder builder = imports();
    builder.append("OrderFilter := SELECT * FROM Orders WHERE \"time\" > now() - INTERVAL 1 YEAR;\n");

    builder.append("OrderAgg1 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM OrderFilter o GROUP BY customer, bucket;\n");
//    //The following should be equivalent
    builder.append("OrderAgg2 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o WHERE o.\"time\" > now() - INTERVAL 1 YEAR GROUP BY customer, bucket;\n");
    process(builder.toString());
    validateQueryTable("orderfilter", TableType.STREAM, ExecutionEngine.Type.STREAM,5, 1, TimestampTest.fixed(4), new PullupTest(true,false));
    validateQueryTable("orderagg1", TableType.STREAM, ExecutionEngine.Type.STREAM,3, 2, TimestampTest.fixed(1), new PullupTest(true,false));
    validateQueryTable("orderagg2", TableType.STREAM, ExecutionEngine.Type.STREAM,3, 2, TimestampTest.fixed(1), new PullupTest(true,false));
  }

  /*
  ===== TOPN TESTS ======
   */

  @Test
  public void topNTest() {
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY \"_ingest_time\" DESC;");
    builder.add("Customer.recentOrders := SELECT o.id, o.time FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.\"time\" DESC LIMIT 10;");
    process(builder.toString());
    validateQueryTable("customer", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,6, 1, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build()); //customerid got moved to the front
    validateQueryTable("recentOrders", TableType.STATE, ExecutionEngine.Type.STREAM,4, 2, TimestampTest.fixed(3), PullupTest.builder().hasTopN(true).build());
  }

  @Test
  public void selectDistinctTest() {
    ScriptBuilder builder = imports();
    builder.add("CustomerId := SELECT DISTINCT customerid FROM Customer;");
    builder.add("CustomerOrders := SELECT o.id, c.customerid FROM CustomerId c JOIN Orders o ON o.customerid = c.customerid");
    process(builder.toString());
    validateQueryTable("customerid", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,2, 1, TimestampTest.fixed(1), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("customerorders", TableType.STREAM, ExecutionEngine.Type.STREAM,4, 1, TimestampTest.fixed(3));
  }

  @Test
  public void partitionSelectDistinctTest() {
    //TODO: add distinctAgg test back in once we keep parent state
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;");
    builder.add("Customer.distinctOrders := SELECT DISTINCT o.id FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.id DESC LIMIT 10;");
    builder.add("Customer.distinctOrdersTime := SELECT DISTINCT o.id, o.time FROM Orders o WHERE _.customerid = o.customerid ORDER BY o.time DESC LIMIT 10;");
//    builder.add("Customer.distinctAgg := SELECT COUNT(d.id) FROM _.distinctOrders d");
    process(builder.toString());
    validateQueryTable("customer", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,6, 1, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build()); //customerid got moved to the front
    validateQueryTable("orders", TableType.STREAM, ExecutionEngine.Type.STREAM,6, 1, TimestampTest.fixed(4)); //temporal join fixes timestamp
    validateQueryTable("distinctorders", TableType.STATE, ExecutionEngine.Type.STREAM,3, 2, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("distinctorderstime", TableType.STATE, ExecutionEngine.Type.STREAM,3, 3, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
//    validateQueryTable("distinctAgg", TableType.TEMPORAL_STATE,3, 2, TimestampTest.fixed(2));
  }

  @Test
  public void distinctTest() {
    ScriptBuilder builder = imports();
    builder.add("Orders := DISTINCT Orders ON id ORDER BY \"time\" DESC");
    process(builder.toString());
    validateQueryTable("orders", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,6, 1, TimestampTest.fixed(4)); //pullup is inlined because nested
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
    builder.add("StreamCount := SELECT round_to_day(rowtime) as day, COUNT(1) as num FROM CombinedStream GROUP BY day");
    process(builder.toString());
    validateQueryTable("combinedstream", TableType.STREAM, ExecutionEngine.Type.STREAM,3, 1, TimestampTest.fixed(2));
    validateQueryTable("streamcount", TableType.STREAM, ExecutionEngine.Type.STREAM,2, 1, TimestampTest.fixed(0));
  }

  @Test
  public void testUnionWithoutTimestamp() {
    ScriptBuilder builder = imports();
    builder.add("CombinedStream := (SELECT o.customerid FROM Orders o)" +
            " UNION ALL " +
            "(SELECT c.customerid FROM Customer c);");
    process(builder.toString());
    validateQueryTable("combinedstream", TableType.STREAM, ExecutionEngine.Type.STREAM,3, 1, TimestampTest.fixed(2));
  }

  /*
  ===== STREAM TESTS ======
   */

  @Test
  public void convert2StreamTest() {
    ScriptBuilder builder = imports();
    builder.append("Product.updateTime := _ingest_time - INTERVAL 1 YEAR");
    builder.append("Product := DISTINCT Product ON productid ORDER BY updateTime DESC");
    builder.append("ProductCount := SELECT p.productid, p.name, SUM(e.quantity) as quantity FROM Orders.entries e JOIN Product p on e.productid = p.productid GROUP BY p.productid, p.name");
    builder.append("CountStream := STREAM ON ADD AS SELECT productid, name, quantity FROM ProductCount WHERE quantity > 1");
    builder.append("ProductCount2 := DISTINCT CountStream ON productid ORDER BY _ingest_time DESC");
    process(builder.toString());
    validateQueryTable("productcount", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,4, 2, TimestampTest.fixed(3));
    validateQueryTable("countstream", TableType.STREAM, ExecutionEngine.Type.STREAM,5, 1, TimestampTest.fixed(1));
    validateQueryTable("productcount2", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM,5, 1, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
  }


  /*
  ===== HINT TESTS ======
   */

  @Test
  public void testExecutionTypeHint() {
    ScriptBuilder builder = new ScriptBuilder();
    builder.add("IMPORT ecommerce-data.Customer");
    builder.add("CustomerAgg1 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");
    builder.add("/*+ EXEC_DB */ CustomerAgg2 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");
    builder.add("/*+ EXEC_STREAM */ CustomerAgg3 := SELECT customerid, COUNT(1) as num FROM Customer WHERE _ingest_time > NOW() - INTERVAL 1 HOUR GROUP BY customerid");

    process(builder.getScript());
    validateQueryTable("customeragg1", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 3, 1, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("customeragg2", TableType.TEMPORAL_STATE, ExecutionEngine.Type.DATABASE, 3, 1, TimestampTest.fixed(2));
    validateQueryTable("customeragg3", TableType.TEMPORAL_STATE, ExecutionEngine.Type.STREAM, 3, 1, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
  }

  private ScriptBuilder imports() {
    return example.getImports();
  }

  @SneakyThrows
  private Resolve.Env process(String query) {
    ScriptNode node = parse(query);
//    SqlNode parsed = SqlParser.create("SELECT o2._uuid FROM Orders o2 \n"
//        + "INNER JOIN (SELECT _uuid FROM Orders) AS o ON o._uuid = o2._uuid\n")
//        .parseQuery();
//    System.out.println(((QueryAssignment) node.getStatements().get(4)).getQuery());
//    System.out.println(parsed);
    resolvedDag = resolve.planDag(session, node);
    return resolvedDag;
  }

  private ScriptNode parse(String query) {
    return parser.parse(query);
  }


  private void validateQueryTable(String name, TableType tableType, ExecutionEngine.Type execType,
                                  int numCols, int numPrimaryKeys) {
    validateQueryTable(name,tableType,execType,numCols,numPrimaryKeys,TimestampTest.NO_TEST,PullupTest.EMPTY);
  }

  private void validateQueryTable(String name, TableType tableType, ExecutionEngine.Type execType,
                                  int numCols, int numPrimaryKeys, TimestampTest timestampTest) {
    validateQueryTable(name,tableType,execType,numCols,numPrimaryKeys,timestampTest,PullupTest.EMPTY);
  }

  private void validateQueryTable(String tableName, TableType tableType, ExecutionEngine.Type execType,
                                  int numCols, int numPrimaryKeys, TimestampTest timestampTest,
                                  PullupTest pullupTest) {
    CalciteSchema relSchema = resolvedDag.getRelSchema();
    QueryRelationalTable table = getLatestTable(relSchema,tableName,QueryRelationalTable.class).get();
    snapshot.addContent(table.getRelNode(),tableName,"lp");
    assertEquals(tableType, table.getType(), "table type");
    assertEquals(table.getExecution().getEngine().getType(),execType,"execution type");
    assertEquals(numPrimaryKeys, table.getNumPrimaryKeys(), "primary key size");
    assertEquals(numCols, table.getRowType().getFieldCount(), "field count");
    timestampTest.test(table.getTimestamp());
    pullupTest.test(table.getPullups());
  }

  public static<T extends AbstractRelationalTable> Optional<T> getLatestTable(CalciteSchema relSchema, String tableName, Class<T> tableClass) {
    String normalizedName = Name.system(tableName).getCanonical();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    return relSchema.getTableNames().stream().filter(s->s.indexOf(Name.NAME_DELIMITER) != -1).filter(s -> s.substring(0,s.indexOf(Name.NAME_DELIMITER)).equals(normalizedName))
            .filter(s -> tableClass.isInstance(relSchema.getTable(s,true).getTable()))
            //Get most recently added table
            .sorted((a,b) -> -Integer.compare(CalciteTableFactory.getTableOrdinal(a),CalciteTableFactory.getTableOrdinal(b)))
            .findFirst().map(s -> tableClass.cast(relSchema.getTable(s,true).getTable()));
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
      this(hasNowFilter,hasTopN,false);
    }

    public void test(PullupOperator.Container pullups) {
      assertEquals(hasNowFilter, !pullups.getNowFilter().isEmpty(), "now filter");
      assertEquals(hasTopN, !pullups.getTopN().isEmpty(), "topN");
      assertEquals(hasSort, !pullups.getSort().isEmpty(), "sort");
    }

  }

  @Value
  public static class TimestampTest {

    public static final TimestampTest NONE = new TimestampTest(Type.NONE,new Integer[0]);
    public static final TimestampTest NO_TEST = new TimestampTest(Type.NO_TEST,new Integer[0]);

    enum Type {NO_TEST, NONE, FIXED, CANDIDATES, BEST }

    final Type type;
    final Integer[] candidates;

    public static TimestampTest best(Integer best) {
      return new TimestampTest(Type.BEST,new Integer[]{best});
    }

    public static TimestampTest fixed(Integer fixed) {
      return new TimestampTest(Type.FIXED,new Integer[]{fixed});
    }

    public static TimestampTest candidates(Integer... candidates) {
      return new TimestampTest(Type.CANDIDATES,candidates);
    }

    public void test(TimestampHolder.Base timeHolder) {
      if (type==Type.NO_TEST) return;
      if (type==Type.NONE) {
        assertFalse(timeHolder.hasFixedTimestamp(), "no timestamp");
        assertEquals(0,timeHolder.getCandidates().size(), "candidate size");
      } else if (type==Type.FIXED) {
        assertTrue(timeHolder.hasFixedTimestamp(), "has timestamp");
        assertEquals(1,candidates.length, "candidate size");
        assertEquals(candidates[0],timeHolder.getTimestampCandidate().getIndex(), "timestamp index");
      } else if (type==Type.BEST) {
        assertFalse(timeHolder.hasFixedTimestamp(), "no timestamp");
        assertEquals(1,candidates.length, "candidate size");
        assertEquals(candidates[0],timeHolder.getBestCandidate().getIndex(), "best candidate");
      } else if (type==Type.CANDIDATES) {
        assertFalse(timeHolder.hasFixedTimestamp(), "no timestamp");
        assertEquals(Sets.newHashSet(candidates),
                timeHolder.getCandidates().stream().map(TimestampHolder.Candidate::getIndex).collect(Collectors.toSet()), "candidate set");
      }
    }

  }

}
