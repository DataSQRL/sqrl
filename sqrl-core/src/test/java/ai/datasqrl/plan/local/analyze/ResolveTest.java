package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.calcite.table.TableType;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.data.C360;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ResolveTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;
  ErrorCollector error;
  private Resolve resolve;
  private Session session;

  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.INSTANCE;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    Assertions.assertTrue(
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
    //Test that we can pull a now() filter through
    //builder.append("OrderAgg2 := SELECT o.customerid as customer, round_to_second(o.\"time\") as bucket, COUNT(o.id) as order_count FROM Orders o WHERE o.time > now() - INTERVAL 1 YEAR GROUP BY customer, bucket;\n");
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

  private void validateQueryTable(String name, TableType tableType, int numCols, int numPrimaryKeys) {
    SchemaPlus relSchema = session.getPlanner().getDefaultSchema();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    List<String> tblNames = relSchema.getTableNames().stream().filter(s -> s.startsWith(name))
        .filter(s -> relSchema.getTable(s) instanceof QueryRelationalTable)
        .collect(Collectors.toList());
    assertFalse(tblNames.isEmpty());
    QueryRelationalTable table = (QueryRelationalTable) relSchema.getTable(tblNames.get(0));
    assertEquals(tableType, table.getType());
    assertEquals(numPrimaryKeys, table.getNumPrimaryKeys());
    assertEquals(numCols, table.getRowType().getFieldCount());
  }

  @Test
  @Disabled
  public void distinctTest() {
    StringBuilder builder = imports();
    builder.append("Orders := DISTINCT Orders ON id ORDER BY _ingest_time DESC;\n");
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
    process("IMPORT ecommerce-data.Orders TIMESTAMP \"time\" + INTERVAL '5' YEAR AS x;\n");
  }

  @Test
  public void standardLibraryTest() {
    StringBuilder builder = imports();
    builder.append("Orders.fnc_test := SELECT \"time\".ROUNDTOMONTH(\"time\") FROM _;");
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
    builder.append("Orders := " + "SELECT o._uuid " + "FROM Orders o2 "
        + "INNER JOIN (SELECT _uuid FROM Orders) o ON o._uuid = o2._uuid;\n");
    process(builder.toString());
    validateQueryTable("orders", TableType.STREAM, 3, 1);
  }

  @Test
  @Disabled
  public void fullTest() {
    StringBuilder builder = imports();
    builder.append("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n");
    builder.append("Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n");
    builder.append("Orders.entries.discount := coalesce(discount, 0.0);\n");
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
        + "                            WHERE parent.\"time\" > now() - INTERVAL '2' YEAR\n"
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
        + "                     SELECT \"time\".roundToMonth(e.parent.\"time\") AS \"month\","
        + "                            e.product.category AS category,"
        + "                            sum(total) AS total,"
        + "                            sum(discount) AS savings"
        + "                     FROM _ JOIN _.orders.entries e"
        + "                     GROUP BY \"month\", category ORDER BY \"month\" DESC;\n");

    builder.append("Customer.spending_by_month :="
        + "                    SELECT \"month\", sum(total) AS total, sum(savings) AS savings"
        + "                    FROM _._spending_by_month_category"
        + "                    GROUP BY \"month\" ORDER BY \"month\" DESC;\n");
    builder.append("Customer.spending_by_month.categories :="
        + "    JOIN _.parent._spending_by_month_category c ON c.\"month\"=_.\"month\";\n");
    builder.append("Product._sales_last_week := SELECT SUM(e.quantity)"
        + "                          FROM _.order_entries e"
        + "                          WHERE e.parent.\"time\" > now() - INTERVAL '7' DAY;\n");
    builder.append("Product._sales_last_month := SELECT SUM(e.quantity)"
        + "                          FROM _.order_entries e"
        + "                          WHERE e.parent.\"time\" > now() - INTERVAL '1' MONTH;\n");
    builder.append("Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n");
    builder.append("Category := SELECT DISTINCT category AS name FROM Product;\n");
    builder.append("Category.products := JOIN Product ON _.name = Product.category;\n");
    builder.append(
        "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase >"
            + " 0" + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n");
    builder.append("Customer.favorite_categories := SELECT s.category as category_name,"
        + "                                        sum(s.total) AS total"
        + "                                FROM _._spending_by_month_category s"
        + "                                WHERE s.\"month\" >= now() - INTERVAL 1 YEAR"
        + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n");
    builder.append(
        "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category.name;\n");
    process(builder.toString());
  }

  private void process(String query) {
    ScriptNode node = parse(query);
    resolve.planDag(session, node);
  }

  private ScriptNode parse(String query) {
    return parser.parse(query);
  }
}
