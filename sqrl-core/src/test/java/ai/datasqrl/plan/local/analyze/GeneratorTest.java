package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.SqrlStatement;
import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.local.generate.Generator;
import ai.datasqrl.plan.local.generate.GeneratorBuilder;
import ai.datasqrl.util.data.C360;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class GeneratorTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;
  ErrorCollector error;
  private Generator generator;

  @BeforeEach
  public void setup() throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.INSTANCE;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    Assertions.assertTrue(importManager.registerUserSchema(bundle.getMainScript().getSchema(),
        error));

    this.generator = GeneratorBuilder.build(importManager, error);
    this.parser = new ConfiguredSqrlParser(error);
  }

  @Test
  public void tableDefinitionTest() {
    gen("IMPORT ecommerce-data.Orders;\n");
    gen("EntryCount := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;");
    validateQueryTable("entrycount",5, 2); //5 cols = 1 select col + 2 pk cols + 2 timestamp cols
  }

  private void validateQueryTable(String name, int numCols, int numPrimaryKeys) {
    CalciteSchema relSchema = generator.getRelSchema();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    List<String> tblNames = relSchema.getTableNames().stream().filter(s -> s.startsWith(name))
            .filter(s -> relSchema.getTable(s,false).getTable() instanceof QueryRelationalTable)
            .collect(Collectors.toList());
    assertFalse(tblNames.isEmpty());
    QueryRelationalTable table = (QueryRelationalTable) relSchema.getTable(tblNames.get(0),false).getTable();
    assertEquals(numPrimaryKeys, table.getNumPrimaryKeys());
    assertEquals(numCols, table.getRowType().getFieldCount());
  }

  @Test
  public void distinctTest() {
    imports();
    gen("Orders := DISTINCT Orders o ON (o._uuid) ORDER BY o._ingest_time DESC;\n");
  }

  private void imports() {
    SqlNode node;
    node = gen("IMPORT ecommerce-data.Customer;\n");
    node = gen("IMPORT ecommerce-data.Orders;\n");
    node = gen("IMPORT ecommerce-data.Product;\n");
  }

  @Test
  public void timestampTest() {
    gen("IMPORT ecommerce-data.Orders TIMESTAMP \"time\" + INTERVAL '5' YEAR AS x;\n");
  }

  @Test
  public void standardLibraryTest() {
    imports();
    gen("Orders.fnc_test := SELECT \"time\".ROUNDTOMONTH(\"time\") FROM _;");
  }

  @Test
  public void joinDeclarationTest() {
    imports();
    gen("Product.p := JOIN Product p ORDER BY p.category LIMIT 1");
    gen("Product.p2 := SELECT * FROM _ JOIN _.p");
  }

  @Test
  public void subqueryTest() {
    SqlNode node;
    node = gen("IMPORT ecommerce-data.Orders;\n");
    node = gen("Orders := "
        + "SELECT o._uuid "
        + "FROM Orders o2 "
        + "INNER JOIN (SELECT _uuid FROM Orders) o ON o._uuid = o2._uuid;\n");
  }

  @Test
  public void fullTest() {
    gen("IMPORT ecommerce-data.Customer;\n");
    gen("IMPORT ecommerce-data.Product;\n");
    gen("IMPORT ecommerce-data.Orders;\n");
    SqlNode node;

//    node = gen("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n");
//    assertEquals(
//        "SELECT `EXPR$0`.`_uuid`, `EXPR$0`.`_ingest_time`, `EXPR$0`.`customerid`, `EXPR$0`"
//            + ".`email`, `EXPR$0`.`name`\n"
//            + "FROM (SELECT `customer$1`.`_uuid`, `customer$1`.`_ingest_time`, `customer$1`"
//            + ".`customerid`, `customer$1`.`email`, `customer$1`.`name`, ROW_NUMBER() OVER "
//            + "(PARTITION BY `customer$1`.`customerid` ORDER BY `customer$1`.`_ingest_time` DESC)"
//            + " AS `_row_num`\n"
//            + "FROM `test`.`customer$1`) AS `EXPR$0`\n"
//            + "WHERE `EXPR$0`.`_row_num` = 1",
//        node.toString());

//    node = gen("Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n");
//    assertEquals(
//        "SELECT `EXPR$0`.`_uuid`, `EXPR$0`.`_ingest_time`, `EXPR$0`.`productid`, `EXPR$0`.`name`,"
//            + " `EXPR$0`.`description`, `EXPR$0`.`category`\n"
//            + "FROM (SELECT `product$2`.`_uuid`, `product$2`.`_ingest_time`, `product$2`"
//            + ".`productid`, `product$2`.`name`, `product$2`.`description`, `product$2`"
//            + ".`category`, ROW_NUMBER() OVER (PARTITION BY `product$2`.`productid` ORDER BY "
//            + "`product$2`.`_ingest_time` DESC) AS `_row_num`\n"
//            + "FROM `test`.`product$2`) AS `EXPR$0`\n"
//            + "WHERE `EXPR$0`.`_row_num` = 1",
//        node.toString());

    node = gen("Orders.entries.discount := coalesce(discount, 0.0);\n");
    System.out.println(node);
//    assertEquals(
//        "CASE WHEN `entries$4`.`discount` IS NOT NULL THEN `entries$4`.`discount` ELSE 0.0 END AS"
//            + " `discount$1`",
//        node.toString());
    node = gen("Orders.entries.total := quantity * unit_price - discount;\n");
//    assertEquals(
//        "`entries$4`.`quantity` * `entries$4`.`unit_price` - `entries$4`.`discount$1` AS `total`",
//        node.toString());
    node = gen("Orders.total := sum(entries.unit_price); \n");
//    assertEquals(
//        "SELECT `_`.`_uuid`, SUM(`t`.`total`) AS `__t0`\n"
//            + "FROM `test`.`orders$3` AS `_`\n"
//            + "LEFT JOIN `test`.`entries$4` AS `t` ON `_`.`_uuid` = `t`.`_uuid`\n"
//            + "GROUP BY `_`.`_uuid`",
//        node.toString());
    node = gen("Orders.total_savings := sum(entries.discount);\n");
//    node = gen("Orders.total_entries := count(entries);\n");
    node = gen("Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n");
    node = gen("Orders.entries.product := JOIN Product ON Product.productid = _.productid LIMIT 1;\n");
    node = gen( "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n");
    node = gen("Customer.recent_products := SELECT productid, e.product.category AS category\n"
//        + "                                   sum(quantity) AS quantity, count(e._idx) AS num_orders\n"
        + "                            FROM _ JOIN _.orders.entries e\n"
//        + "                            WHERE parent.\"time\" > now() - INTERVAL '2' YEAR\n"
//        + "                            GROUP BY productid, category ORDER BY num_orders DESC, "
//        + "quantity DESC;\n");
    );
    node = gen("Customer.recent_products_categories :="
        + "                     SELECT category, count(e.productid) AS num_products"
        + "                     FROM _ JOIN _.recent_products"
        + "                     GROUP BY category ORDER BY num_products;\n");
    node = gen(
        "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp"
            + ".category=_.category;\n");
    node = gen("Customer._spending_by_month_category :="
        + "                     SELECT time.roundToMonth(e.parent.\"time\") AS \"month\","
        + "                            e.product.category AS category,"
        + "                            sum(total) AS total,"
        + "                            sum(discount) AS savings"
        + "                     FROM _ JOIN _.orders.entries e"
        + "                     GROUP BY \"month\", category ORDER BY \"month\" DESC;\n");

    node = gen("Customer.spending_by_month :="
        + "                    SELECT month, sum(total) AS total, sum(savings) AS savings"
        + "                    FROM _._spending_by_month_category"
        + "                    GROUP BY month ORDER BY month DESC;\n");
    node = gen("Customer.spending_by_month.categories :="
        + "    JOIN _.parent._spending_by_month_category c ON c.\"month\"=_.\"month\";\n");
    node = gen("Product._sales_last_week := SELECT SUM(e.quantity)"
        + "                          FROM _.order_entries e"
        + "                          WHERE e.parent.\"time\" > now() - INTERVAL '7' DAY;\n");
    node = gen("Product._sales_last_month := SELECT SUM(e.quantity)"
        + "                          FROM _.order_entries e"
        + "                          WHERE e.parent.\"time\" > now() - INTERVAL '1' MONTH;\n");
    node = gen("Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n");
    node = gen("Category := SELECT DISTINCT category AS name FROM Product;\n");
    node = gen("Category.products := JOIN Product ON _.name = Product.category;\n");
    node = gen(
        "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase >"
            + " 0"
            + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n");
//    node = gen("Customer.favorite_categories := SELECT s.category as category_name,"
//        + "                                        sum(s.total) AS total"
//        + "                                FROM _._spending_by_month_category s"
//        + "                                WHERE s.month >= now() - INTERVAL 1 YEAR"
//        + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n");
//    node = gen("Customer.favorite_categories.category := JOIN Category ON _.category_name = Category.name;\n");
////            + "CREATE SUBSCRIPTION NewCustomerPromotion ON ADD AS "
////            + "SELECT customerid, email, name, total_orders FROM Customer WHERE total_orders >=
////            100;\n";
    System.out.println();
  }

  private SqlNode gen(String query) {
    SqrlStatement imp = parse(query);
    generator.generate(imp);
    return null;
  }

  private SqrlStatement parse(String query) {
    return (SqrlStatement) parser.parse(query).getStatements().get(0);
  }
}