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
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    C360 example = C360.INSTANCE;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    assertTrue(
        importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));
    Planner planner = new PlannerFactory(
        CalciteSchema.createRootSchema(false, false).plus()).createPlanner();
    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve();
  }

  @Test
  public void tableImportTest() {
    process(imports().toString());
    validateQueryTable("customer", TableType.STREAM, 5, 1);
    validateQueryTable("product", TableType.STREAM,6, 1);
    validateQueryTable("orders", TableType.STREAM,6, 1);
  }

  @Test
  public void simpleColumnDefinition() {
    String script = "IMPORT ecommerce-data.Customer;\n"
            + "Customer.timestamp := EPOCH_TO_TIMESTAMP(lastUpdated);\n";
    process(script);
    validateQueryTable("customer", TableType.STREAM, 6, 1, Optional.of(5), PullupTest.EMPTY);
  }


  @Test
  public void tableDefinitionTest() {
    String sqrl = "IMPORT ecommerce-data.Orders;\n"
        + "EntryCount := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;";
    process(sqrl);
    validateQueryTable("entrycount", TableType.STREAM,5, 2); //5 cols = 1 select col + 2 pk cols + 2 timestamp cols
  }

  @Test
  public void tableJoinTest() {
    StringBuilder builder = imports();
    builder.append("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    process(builder.toString());
    validateQueryTable("ordercustomer", TableType.STATE,5, 2); //numCols = 3 selected cols + 2 uuid cols for pk
  }

  @Test
  public void tableIntervalJoinTest() {
    StringBuilder builder = imports();
    builder.append("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o INTERVAL JOIN Customer c on o.customerid = c.customerid "+
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    builder.append("OrderCustomer2 := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid "+
            "AND o.\"time\" > c.\"_ingest_time\" AND o.\"time\" <= c.\"_ingest_time\" + INTERVAL 2 MONTH;");
    process(builder.toString());
    validateQueryTable("ordercustomer", TableType.STREAM,6, 2); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
    validateQueryTable("ordercustomer2", TableType.STREAM,6, 2); //numCols = 3 selected cols + 2 uuid cols for pk + 1 for timestamp
  }

  @Test
  public void tableTemporalJoinTest() {
    StringBuilder builder = imports();
    builder.append("CustomerCount := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer;\n");
    builder.append("OrderWithCount := SELECT o.id, c.order_count, o.customerid FROM Orders o TEMPORAL JOIN CustomerCount c on o.customerid = c.customer;");
    builder.append("OrderWithCount2 := SELECT o.id, c.order_count, o.customerid FROM CustomerCount c TEMPORAL JOIN Orders o on o.customerid = c.customer;");
    process(builder.toString());
    validateQueryTable("orderwithcount", TableType.STREAM,5, 1); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
    validateQueryTable("orderwithcount2", TableType.STREAM,5, 1); //numCols = 3 selected cols + 1 uuid cols for pk + 1 for timestamp
  }

  @Test
  public void streamAggregateTest() {
    StringBuilder builder = imports();
    builder.append("OrderAgg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM Orders o GROUP BY customer;\n");
    builder.append("OrderAgg2 := SELECT COUNT(o.id) as order_count FROM Orders o;");
    process(builder.toString());
    validateQueryTable("orderagg1", TableType.TEMPORAL_STATE,3, 1); //timestamp column is added
    validateQueryTable("orderagg2", TableType.TEMPORAL_STATE,2, 0);
  }

  @Test
  public void streamTimeAggregateTest() {
    StringBuilder builder = imports();
    builder.append("OrderAgg1 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o GROUP BY customer, bucket;\n");
    process(builder.toString());
    validateQueryTable("orderagg1", TableType.STREAM,3, 2);
  }

  @Test
  public void timeFunctionFixTest() {
    StringBuilder builder = imports();
    builder.append("TimeTest := SELECT ROUND_TO_MONTH(ROUND_TO_MONTH(\"time\")) FROM orders;");
    process(builder.toString());
  }

  @Test
  public void streamStateAggregateTest() {
    StringBuilder builder = imports();
    builder.append("OrderCustomer := SELECT o.id, c.name, o.customerid FROM Orders o JOIN Customer c on o.customerid = c.customerid;");
    builder.append("agg1 := SELECT o.customerid as customer, COUNT(o.id) as order_count FROM OrderCustomer o GROUP BY customer;\n");
    process(builder.toString());
    validateQueryTable("agg1", TableType.STATE,2, 1);
  }

  @Test
  public void nowFilterTest() {
    StringBuilder builder = imports();
    builder.append("OrderFilter := SELECT * FROM Orders WHERE \"time\" > now() - INTERVAL 1 YEAR;\n");

    builder.append("OrderAgg1 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM OrderFilter o GROUP BY customer, bucket;\n");
//    //The following should be equivalent
    builder.append("OrderAgg2 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o WHERE o.\"time\" > now() - INTERVAL 1 YEAR GROUP BY customer, bucket;\n");
    process(builder.toString());
    validateQueryTable("orderfilter", TableType.STREAM,5, 1, Optional.of(4), new PullupTest(true,false));
    validateQueryTable("orderagg1", TableType.STREAM,3, 2, Optional.of(1), new PullupTest(true,false));
    validateQueryTable("orderagg2", TableType.STREAM,3, 2, Optional.of(1), new PullupTest(true,false));
  }


  @Test
  @Disabled
  public void distinctTest() {
    StringBuilder builder = imports();
    builder.append("Orders := DISTINCT Orders ON id ORDER BY \"time\" DESC;\n");
    process(builder.toString());
  }

  private StringBuilder imports() {
    StringBuilder builder = new StringBuilder();
    builder.append("IMPORT ecommerce-data.Customer;\n");
    builder.append("IMPORT ecommerce-data.Orders;\n");
    builder.append("IMPORT ecommerce-data.Product;\n");
    return builder;
  }

  @Test
  @Disabled
  public void timestampTest() {
    process("IMPORT ecommerce-data.Orders TIMESTAMP time + INTERVAL 5 YEAR AS x;\n");
  }

  @Test
  @Disabled
  public void standardLibraryTest() {
    StringBuilder builder = imports();
    builder.append("Orders.fnc_test := SELECT time.ROUNDTOMONTH(time) FROM _;");
    process(builder.toString());
  }

  @Test
  @Disabled
  public void joinDeclarationTest() {
    StringBuilder builder = imports();
    builder.append("Product.p := JOIN Product p ORDER BY p.category LIMIT 1");
    builder.append("Product.p2 := SELECT * FROM _ JOIN _.p");
    process(builder.toString());
  }

  @Test
  public void subqueryTest() {
    StringBuilder builder = imports();
    builder.append("IMPORT ecommerce-data.Orders;\n");
    builder.append("Orders := SELECT o2._uuid FROM Orders o2 "
        + "INNER JOIN (SELECT _uuid FROM Orders) AS o ON o._uuid = o2._uuid;\n");
    process(builder.toString());
    validateQueryTable("orders", TableType.STREAM, 3, 1);
  }

  @Test
  @Disabled
  public void fullTest() {
    StringBuilder builder = imports();
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
    validateQueryTable(name,tableType,numCols,numPrimaryKeys,Optional.empty(),PullupTest.EMPTY);
  }

  private void validateQueryTable(String tableName, TableType tableType, int numCols, int numPrimaryKeys,
                                  Optional<Integer> timestampIdx,
                                  PullupTest pullupTest) {
    CalciteSchema relSchema = resolvedDag.getRelSchema();
    QueryRelationalTable table = getLatestTable(relSchema,tableName,QueryRelationalTable.class).get();
    assertEquals(tableType, table.getType());
    assertEquals(numPrimaryKeys, table.getNumPrimaryKeys());
    assertEquals(numCols, table.getRowType().getFieldCount());
    if (timestampIdx.isPresent()) {
      assertTrue(table.getTimestamp().hasTimestamp());
      assertEquals(timestampIdx.get(),table.getTimestamp().getTimestampIndex());
    }
    pullupTest.test(table.getPullups());
  }

  public static<T extends AbstractRelationalTable> Optional<T> getLatestTable(CalciteSchema relSchema, String tableName, Class<T> tableClass) {
    String normalizedName = Name.system(tableName).getCanonical();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    return relSchema.getTableNames().stream().filter(s -> s.startsWith(normalizedName))
            .filter(s -> tableClass.isInstance(relSchema.getTable(s,true).getTable()))
            //Get most recently added table
            .sorted((a,b) -> -Integer.compare(CalciteTableFactory.getTableOrdinal(a),CalciteTableFactory.getTableOrdinal(b)))
            .findFirst().map(s -> tableClass.cast(relSchema.getTable(s,true).getTable()));
  }

  @Value
  public static class PullupTest {

    public static final PullupTest EMPTY = new PullupTest(false, false);

    boolean hasNowFilter;
    boolean hasDeduplication;

    public void test(PullupOperator.Container pullups) {
      assertEquals(hasNowFilter, !pullups.getNowFilter().isEmpty());
      assertEquals(hasDeduplication, !pullups.getDeduplication().isEmpty());
    }

  }
}
