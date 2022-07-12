package ai.datasqrl.plan.local.analyzer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.SqrlStatement;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.util.data.C360;
import java.io.IOException;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.BridgedCalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Generator2Test extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;

  ErrorCollector errorCollector;
  ImportManager importManager;
  Analyzer analyzer;
  Analysis analysis;
  private Planner planner;
  //  private ScriptNode script;
  private Generator generator;

  @BeforeEach
  public void setup() throws IOException {
    errorCollector = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.INSTANCE;

    example.registerSource(env);

    importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    Assertions.assertTrue(importManager.registerUserSchema(bundle.getMainScript().getSchema(),
        ErrorCollector.root()));
    parser = ConfiguredSqrlParser.newParser(errorCollector);
    analyzer = new Analyzer(importManager, SchemaAdjustmentSettings.DEFAULT,
        errorCollector);

    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    String schemaName = "test";
    BridgedCalciteSchema subSchema = new BridgedCalciteSchema();
    rootSchema.add(schemaName, subSchema)
        .add(schemaName, subSchema); //also give the subschema access

    PlannerFactory plannerFactory = new PlannerFactory(rootSchema);
    Planner planner = plannerFactory.createPlanner(schemaName);
    this.planner = planner;

//    String query =
//        "IMPORT ecommerce-data.Customer;\n"
//            + "IMPORT ecommerce-data.Product;\n"
//            + "IMPORT ecommerce-data.Orders;\n"
//            + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
//            + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
//            + "Orders.entries.discount := coalesce(discount, 0.0);\n"
//            + "Orders.entries.total := quantity * unit_price - discount;\n"
//            + "Orders.total := sum(entries.total);\n"
//            + "Orders.total_savings := sum(entries.discount);\n"
//            + "Orders.total_entries := count(entries);\n"
//            + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
//            + "Customer.total_orders := sum(_.orders.total);\n"
//            + "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
//            + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n"
//            + "Customer.recent_products := SELECT productid, e.product.category AS category,"
//            + "                                   sum(quantity) AS quantity, count(*) AS
//            num_orders"
//            + "                            FROM _.orders.entries e"
//            + "                            WHERE parent.time > now() - INTERVAL 2 YEAR"
//            + "                            GROUP BY productid, category ORDER BY num_orders
//            DESC, "
//            + "quantity DESC;\n"
//            + "Customer.recent_products_categories :="
//            + "                     SELECT category, count(*) AS num_products"
//            + "                     FROM _.recent_products"
//            + "                     GROUP BY category ORDER BY num_products;\n"
//            + "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp
//            ON rp"
//            + ".category=_.category;\n"
//            + "Customer._spending_by_month_category :="
//            + "                     SELECT time.roundToMonth(parent.time) AS month,"
//            + "                            e.product.category AS category,"
//            + "                            sum(total) AS total,"
//            + "                            sum(discount) AS savings"
//            + "                     FROM _.orders.entries e"
//            + "                     GROUP BY month, category ORDER BY month DESC;\n"
//            + "Customer.spending_by_month :="
//            + "                    SELECT month, sum(total) AS total, sum(savings) AS savings"
//            + "                    FROM _._spending_by_month_category"
//            + "                    GROUP BY month ORDER BY month DESC;\n"
//            + "Customer.spending_by_month.categories :="
//            + "    JOIN _.parent._spending_by_month_category c ON c.month=month;\n"
//            + "Product._sales_last_week := SELECT SUM(e.quantity)"
//            + "                          FROM _.order_entries e"
//            + "                          WHERE e.parent.time > now() - INTERVAL 7 DAY;\n"
//            + "Product._sales_last_month := SELECT SUM(e.quantity)"
//            + "                          FROM _.order_entries e"
//            + "                          WHERE e.parent.time > now() - INTERVAL 1 MONTH;\n"
//            + "Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n"
//            + "Category := SELECT DISTINCT category AS name FROM Product;\n"
//            + "Category.products := JOIN Product ON _.name = Product.category;\n"
//            + "Category.trending := JOIN Product p ON _.name = p.category AND p
//            ._last_week_increase >"
//            + " 0"
//            + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n"
//            + "Customer.favorite_categories := SELECT s.category as category_name,"
//            + "                                        sum(s.total) AS total"
//            + "                                FROM _._spending_by_month_category s"
//            + "                                WHERE s.month >= now() - INTERVAL 1 YEAR"
//            + "                                GROUP BY category_name ORDER BY total DESC LIMIT
//            5;\n"
//            + "Customer.favorite_categories.category := JOIN Category ON _.category_name =
//            Category"
//            + ".name;\n"
//            + "CREATE SUBSCRIPTION NewCustomerPromotion ON ADD AS "
//            + "SELECT customerid, email, name, total_orders FROM Customer WHERE total_orders >=
//            100;\n";
//    script = parser.parse(query);
    generator = new Generator(planner, analyzer.getAnalysis());
    subSchema.setBridge(generator);
  }

  @Test
  public void distinctTest() {
    gen("IMPORT ecommerce-data.Customer;\n");
    gen("IMPORT ecommerce-data.Product;\n");
    gen("IMPORT ecommerce-data.Orders;\n");
    SqlNode node;
//
//    SqlNode node = gen("Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n");
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
//
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
//
//    node = gen("Orders.entries.discount := coalesce(discount, 0.0);\n");
//    assertEquals(
//        "CASE WHEN `entries$4`.`discount` IS NOT NULL THEN `entries$4`.`discount` ELSE 0.0 END AS"
//            + " `discount$1`",
//        node.toString());
//    node = gen("Orders.entries.total := quantity * unit_price - discount;\n");
//    assertEquals(
//        "`entries$4`.`quantity` * `entries$4`.`unit_price` - `entries$4`.`discount$1` AS `total`",
//        node.toString());
//    node = gen("Orders.total := sum(entries.total);\n");
//    assertEquals(
//        "SELECT `_`.`_uuid`, SUM(`t`.`total`) AS `__t0`\n"
//            + "FROM `test`.`orders$3` AS `_`\n"
//            + "LEFT JOIN `test`.`entries$4` AS `t` ON `_`.`_uuid` = `t`.`_uuid`\n"
//            + "GROUP BY `_`.`_uuid`",
//        node.toString());
//    node = gen("Orders.total_savings := sum(entries.discount);\n");
////    node = gen("Orders.total_entries := count(entries);\n");
    node = gen("Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n");
    node = gen("Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n");
    node = gen("Customer.recent_products := SELECT productid, e.product.category AS category,"
        + "                                   sum(quantity) AS quantity, count(*) AS num_orders"
        + "                            FROM _.orders.entries e"
//        + "                            WHERE parent.time > now() - INTERVAL 2 YEAR");
        + "                            GROUP BY productid, category ORDER BY num_orders DESC, "
        + "quantity DESC;\n");
    node = gen("Customer.recent_products_categories :="
        + "                     SELECT category, count(*) AS num_products"
        + "                     FROM _.recent_products");
//        + "                     GROUP BY category ORDER BY num_products;\n");
    node = gen(
        "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp"
            + ".category=_.category;\n");
    node = gen("Customer._spending_by_month_category :="
        + "                     SELECT time.roundToMonth(parent.time) AS month,"
        + "                            e.product.category AS category,"
        + "                            sum(total) AS total,"
        + "                            sum(discount) AS savings"
        + "                     FROM _.orders.entries e"
        + "                     GROUP BY month, category ORDER BY month DESC;\n");
  }

  private SqlNode gen(String query) {
    SqrlStatement imp = parse(query);
    analyzer.analyze(imp);
    return generator.generate(imp);
  }

  private SqrlStatement parse(String query) {
    return (SqrlStatement) parser.parse(query).getStatements().get(0);
  }
}