package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.*;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.data.C360;
import ai.datasqrl.util.ScriptBuilder;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.ScriptNode;
import org.apache.commons.compress.utils.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.Collectors;

import static ai.datasqrl.util.data.C360.RETAIL_DIR_BASE;
import static org.junit.jupiter.api.Assertions.*;

public class ResolveTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;
  ErrorCollector error;
  private Resolve resolve;
  private Session session;

  private Resolve.Env resolvedDag = null;

  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.BASIC;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().getBundle();
    assertTrue(
        importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));
    Planner planner = new PlannerFactory(
        CalciteSchema.createRootSchema(false, false).plus()).createPlanner();
    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve(RETAIL_DIR_BASE);
  }

  /*
  ===== IMPORT TESTS ======
   */

  @Test
  public void tableImportTest() {
    process(imports().toString());
    validateQueryTable("customer", TableType.STREAM, 6, 1, TimestampTest.best(1));
    validateQueryTable("product", TableType.STREAM,6, 1, TimestampTest.best(1));
    validateQueryTable("orders", TableType.STREAM,6, 1, TimestampTest.best(4));
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
    validateQueryTable("customer", TableType.STREAM, 8, 1, TimestampTest.candidates(1,6,7));
    validateQueryTable("customercopy", TableType.STREAM, 4, 1, TimestampTest.candidates(1,2,3));
  }

  @Test
  public void timestampTest() {
    process("IMPORT ecommerce-data.Customer TIMESTAMP EPOCH_TO_TIMESTAMP(lastUpdated) AS timestamp;\n");
    validateQueryTable("customer", TableType.STREAM, 7, 1, TimestampTest.fixed(6));
  }


  @Test
  public void selfJoinSubqueryTest() {
    ScriptBuilder builder = imports();
//    builder.append("IMPORT ecommerce-data.Orders;\n");
    builder.append("Orders2 := SELECT o2._uuid FROM Orders o2 "
            + "INNER JOIN (SELECT _uuid FROM Orders) AS o ON o._uuid = o2._uuid;\n");
    process(builder.toString());
    validateQueryTable("orders2", TableType.STREAM, 3, 1, TimestampTest.candidates(1,2));
  }

  @Test
  @Disabled //todo: Fix
  public void tableDefinitionTest() {
    String sqrl = ScriptBuilder.of("IMPORT ecommerce-data.Orders",
        "EntryCount := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;");
    process(sqrl);
    validateQueryTable("entrycount", TableType.STREAM,5, 2, TimestampTest.candidates(3,4)); //5 cols = 1 select col + 2 pk cols + 2 timestamp cols
  }


  /*
  ===== JOINS ======
   */

  @Test
  public void tableJoinTest() {
    ScriptBuilder builder = imports();
    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    process(builder.toString());
    validateQueryTable("ordercustomer", TableType.STATE,5, 2, TimestampTest.NONE); //numCols = 3 selected cols + 2 uuid cols for pk
  }

  @Test
  public void tableIntervalJoinTest() {
    ScriptBuilder builder = imports();
    builder.add("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o INTERVAL JOIN Customer c on o.customerid = c.customerid "+
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    builder.append("OrderCustomer2 := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "+
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    process(builder.toString());
    validateQueryTable("ordercustomer", TableType.STREAM,6, 2, TimestampTest.fixed(5)); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
    validateQueryTable("ordercustomer2", TableType.STREAM,6, 2, TimestampTest.fixed(5)); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
  }

  @Test
  public void tableTemporalJoinTest() {
    ScriptBuilder builder = imports();
    builder.append("CustomerCount := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer;\n");
    builder.append("OrderWithCount := SELECT o.id, c.order_count, o.customerid FROM Orders o TEMPORAL JOIN CustomerCount c on o.customerid = c.customer;");
    builder.append("OrderWithCount2 := SELECT o.id, c.order_count, o.customerid FROM CustomerCount c TEMPORAL JOIN Orders o on o.customerid = c.customer;");
    process(builder.toString());
    validateQueryTable("orderwithcount", TableType.STREAM,5, 1, TimestampTest.fixed(4)); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
    validateQueryTable("orderwithcount2", TableType.STREAM,5, 1, TimestampTest.fixed(4)); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
  }

  @Test
  @Disabled
  public void relationshipDeclarationTest() {
    ScriptBuilder builder = imports();
    builder.append("Product.p := JOIN Product p ORDER BY p.category LIMIT 1");
    builder.append("Product.p2 := SELECT * FROM _ JOIN _.p");
    process(builder.toString());
  }


  /*
  ===== AGGREGATE ======
   */

  @Test
  public void streamAggregateTest() {
    ScriptBuilder builder = imports();
    builder.append("OrderAgg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer;\n");
    builder.append("OrderAgg2 := SELECT COUNT(o.id) as order_count FROM Orders o;");
    process(builder.toString());
    validateQueryTable("orderagg1", TableType.TEMPORAL_STATE,3, 1, TimestampTest.fixed(2)); //timestamp column is added
    validateQueryTable("orderagg2", TableType.TEMPORAL_STATE,2, 0, TimestampTest.fixed(1));
  }

  @Test
  public void streamTimeAggregateTest() {
    ScriptBuilder builder = imports();
    builder.append("OrderAgg1 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket;\n");
    process(builder.toString());
    validateQueryTable("orderagg1", TableType.STREAM,3, 2, TimestampTest.fixed(1));
  }

  @Test
  public void streamStateAggregateTest() {
    ScriptBuilder builder = imports();
    builder.append("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    builder.append("agg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");
    process(builder.toString());
    validateQueryTable("agg1", TableType.STATE,2, 1, TimestampTest.NONE);
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
    validateQueryTable("orderfilter", TableType.STREAM,5, 1, TimestampTest.fixed(4), new PullupTest(true,false));
    validateQueryTable("orderagg1", TableType.STREAM,3, 2, TimestampTest.fixed(1), new PullupTest(true,false));
    validateQueryTable("orderagg2", TableType.STREAM,3, 2, TimestampTest.fixed(1), new PullupTest(true,false));
  }

  /*
  ===== TOPN TESTS ======
   */

  @Test
  public void topNTest() {
    //TODO: add orderAgg test back in once this works in transpiler
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY \"_ingest_time\" DESC");
    builder.add("Customer.recentOrders := SELECT o.id, o.time FROM _ JOIN Orders o ON _.customerid = o.customerid ORDER BY o.\"time\" DESC LIMIT 10;");
//    builder.add("Customer.orderAgg := SELECT COUNT(d.id) FROM _ JOIN _.recentOrders d");
    process(builder.toString());
    validateQueryTable("recentOrders", TableType.TEMPORAL_STATE,4, 2, TimestampTest.fixed(3), PullupTest.builder().hasTopN(true).build());
//    validateQueryTable("orderAgg", TableType.TEMPORAL_STATE,3, 2, TimestampTest.fixed(2));
  }

  @Test
  public void selectDistinctTest() {
    ScriptBuilder builder = imports();
    builder.add("CustomerId := SELECT DISTINCT customerid FROM Customer;");
    builder.add("CustomerOrders := SELECT o.id, c.customerid FROM CustomerId c JOIN Orders o ON o.customerid = c.customerid");
    process(builder.toString());
    validateQueryTable("customerid", TableType.TEMPORAL_STATE,2, 1, TimestampTest.fixed(1), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("customerorders", TableType.STREAM,4, 1, TimestampTest.fixed(3));
  }

  @Test
  public void partitionSelectDistinctTest() {
    //TODO: add distinctAgg test back in once this works in transpiler
    ScriptBuilder builder = imports();
    builder.add("Customer := DISTINCT Customer ON customerid ORDER BY \"_ingest_time\" DESC");
    builder.add("Customer.distinctOrders := SELECT DISTINCT o.id FROM _ JOIN Orders o ON _.customerid = o.customerid ORDER BY o.id DESC LIMIT 10;");
    builder.add("Customer.distinctOrdersTime := SELECT DISTINCT o.id, o.\"time\" FROM _ JOIN Orders o ON _.customerid = o.customerid ORDER BY o.\"time\" DESC LIMIT 10;");
//    builder.add("Customer.distinctAgg := SELECT COUNT(d.id) FROM _ JOIN _.distinctOrders d");
    process(builder.toString());
    validateQueryTable("customer", TableType.TEMPORAL_STATE,6, 1, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build()); //customerid got moved to the front
    validateQueryTable("orders", TableType.STREAM,6, 1, TimestampTest.fixed(4)); //temporal join fixes timestamp
    validateQueryTable("distinctorders", TableType.TEMPORAL_STATE,3, 2, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
    validateQueryTable("distinctorderstime", TableType.TEMPORAL_STATE,3, 3, TimestampTest.fixed(2), PullupTest.builder().hasTopN(true).build());
//    validateQueryTable("distinctAgg", TableType.TEMPORAL_STATE,3, 2, TimestampTest.fixed(2));
  }

  @Test
  public void distinctTest() {
    ScriptBuilder builder = imports();
    builder.add("Orders := DISTINCT Orders ON id ORDER BY \"time\" DESC");
    process(builder.toString());
    validateQueryTable("orders", TableType.TEMPORAL_STATE,6, 1, TimestampTest.fixed(4)); //pullup is inlined because nested
  }

  /*
  ===== SET TESTS ======
   */

  @Test
  public void testUnion() {
    ScriptBuilder builder = imports();
    builder.add("CombinedStream := (SELECT o.customerid, o.\"time\" AS rowtime FROM Orders o)" +
            " UNION ALL " +
            "(SELECT c.customerid, c.\"_ingest_time\" AS rowtime FROM Customer c);");
    process(builder.toString());
    validateQueryTable("combinedstream", TableType.STREAM,3, 1, TimestampTest.fixed(2));
  }


  @Test
  @Disabled
  public void fullTest() {
    ScriptBuilder builder = imports();
    builder.append("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n");
    builder.append("Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n");
//    builder.append("Orders.entries.discount := coalesce(discount, 0.0);\n");
    builder.append("Orders.entries.total := quantity * unit_price - discount;\n");
    builder.append("Orders.total := sum(entries.unit_price); \n");
    builder.append("Orders.total_savings := sum(entries.discount);\n");
    builder.append("Orders.total_entries := count(entries);\n");
    builder.append("Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n");
    builder.append(
        "Orders.entries.product := JOIN Product ON Product.productid = _.productid LIMIT 1;\n");
    builder.append(
        "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n");
    builder.append("Customer.recent_products := SELECT productid, e.product.category AS category,\n"
        + "                                   sum(quantity) AS quantity, count(e._idx) AS num_orders\n"
        + "                            FROM _ JOIN _.orders.entries e\n"
        + "                            WHERE parent.time > now() - INTERVAL 2 YEAR\n"
        + "                            GROUP BY productid, category ORDER BY num_orders DESC, "
        + "quantity DESC;\n");

    builder.append("Customer.recent_products_categories :="
        + "                     SELECT category, count(e.productid) AS num_products"
        + "                     FROM _ JOIN _.recent_products"
        + "                     GROUP BY category ORDER BY num_products;\n");
    builder.append(
        "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp"
            + ".category=_.category;\n");
    builder.append("Customer._spending_by_month_category :="
        + "                     SELECT time.roundToMonth(e.parent.time) AS month,"
        + "                            e.product.category AS category,"
        + "                            sum(total) AS total,"
        + "                            sum(discount) AS savings"
        + "                     FROM _ JOIN _.orders.entries e"
        + "                     GROUP BY month, category ORDER BY month DESC;\n");

    builder.append("Customer.spending_by_month :="
        + "                    SELECT month, sum(total) AS total, sum(savings) AS savings"
        + "                    FROM _._spending_by_month_category"
        + "                    GROUP BY month ORDER BY month DESC;\n");
    builder.append("Customer.spending_by_month.categories :="
        + "    JOIN _.parent._spending_by_month_category c ON c.month=_.month;\n");
    builder.append("Product._sales_last_week := SELECT SUM(e.quantity)"
        + "                          FROM _.order_entries e"
        + "                          WHERE e.parent.time > now() - INTERVAL 7 DAY;\n");
    builder.append("Product._sales_last_month := SELECT SUM(e.quantity)"
        + "                          FROM _.order_entries e"
        + "                          WHERE e.parent.time > now() - INTERVAL 1 MONTH;\n");
    builder.append("Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n");
    builder.append("Category := SELECT DISTINCT category AS name FROM Product;\n");
    builder.append("Category.products := JOIN Product ON _.name = Product.category;\n");
    builder.append(
        "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase >"
            + " 0" + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n");
    builder.append("Customer.favorite_categories := SELECT s.category as category_name,"
        + "                                        sum(s.total) AS total"
        + "                                FROM _._spending_by_month_category s"
        + "                                WHERE s.month >= now() - INTERVAL 1 YEAR"
        + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n");
    builder.append(
        "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category.name;\n");
    process(builder.toString());
  }

  private ScriptBuilder imports() {
    return C360.BASIC.getImports();
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


  private void validateQueryTable(String name, TableType tableType, int numCols, int numPrimaryKeys) {
    validateQueryTable(name,tableType,numCols,numPrimaryKeys,TimestampTest.NO_TEST,PullupTest.EMPTY);
  }

  private void validateQueryTable(String name, TableType tableType, int numCols, int numPrimaryKeys,
                                  TimestampTest timestampTest) {
    validateQueryTable(name,tableType,numCols,numPrimaryKeys,timestampTest,PullupTest.EMPTY);
  }

  private void validateQueryTable(String tableName, TableType tableType, int numCols, int numPrimaryKeys,
                                  TimestampTest timestampTest,
                                  PullupTest pullupTest) {
    CalciteSchema relSchema = resolvedDag.getRelSchema();
    QueryRelationalTable table = getLatestTable(relSchema,tableName,QueryRelationalTable.class).get();
    assertEquals(tableType, table.getType(), "table type");
    assertEquals(numPrimaryKeys, table.getNumPrimaryKeys(), "primary key size");
    assertEquals(numCols, table.getRowType().getFieldCount(), "field count");
    timestampTest.test(table.getTimestamp());
    pullupTest.test(table.getPullups());
  }

  public static<T extends AbstractRelationalTable> Optional<T> getLatestTable(CalciteSchema relSchema, String tableName, Class<T> tableClass) {
    String normalizedName = Name.system(tableName).getCanonical();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    return relSchema.getTableNames().stream().filter(s -> s.substring(0,s.indexOf(Name.NAME_DELIMITER)).equals(normalizedName))
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
