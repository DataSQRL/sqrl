package ai.datasqrl.plan.local.analyzer;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.SqrlSchemaCatalog;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.util.data.C360;
import java.io.IOException;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.BridgedCalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GeneratorTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;

  ErrorCollector errorCollector;
  ImportManager importManager;
  Analyzer analyzer;
  private Planner planner;
  BridgedCalciteSchema subSchema;

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
    SqrlSchemaCatalog catalog = new SqrlSchemaCatalog(rootSchema);
    String schemaName = "test";
    subSchema = new BridgedCalciteSchema();
    catalog.add(schemaName, subSchema);

    PlannerFactory plannerFactory = new PlannerFactory(catalog);
    Planner planner = plannerFactory.createPlanner(schemaName);
    this.planner = planner;
  }

  @Test
  public void importTest() {
    ScriptNode scriptNode = parser.parse("IMPORT ecommerce-data.Product;");
    Analysis analysis = analyzer.analyze(scriptNode);
    Generator generator = new Generator(planner, analysis);
    generator.generate(scriptNode);

    Table product = analysis.getSchema().getTable(Name.system("Product")).get();

    Assertions.assertNotNull(generator.getTable(Name.system("ecommerce-data.Product")));
    Assertions.assertNotNull(generator.getTable(product.getId()));
  }

  @Test
  public void testQuery() {
    ScriptNode scriptNode = parser.parse("IMPORT ecommerce-data.Product;"
        + "Product2 := SELECT productid FROM Product;");
    Analysis analysis = analyzer.analyze(scriptNode);
    Generator generator = new Generator(planner, analysis);
    subSchema.setBridge(generator);
    generator.generate(scriptNode);

    Assertions.assertNotNull(generator.getTable(Name.system("ecommerce-data.Product")));
    Assertions.assertNotNull(generator.getTable(Name.system("Product2")));
  }

  @Test
  public void nestedQueryTest() {
    ScriptNode scriptNode = parser.parse("IMPORT ecommerce-data.Product;"
        + "Product.nested := SELECT productid FROM _;");
    Analysis analysis = analyzer.analyze(scriptNode);
    Generator generator = new Generator(planner, analysis);
    subSchema.setBridge(generator);
    generator.generate(scriptNode);

    Field field = analysis.getSchema().getTable(Name.system("Product")).get().getField(Name.system("nested")).get();

    Assertions.assertTrue(field instanceof Relationship);
    Assertions.assertNotNull(generator.getTable(((Relationship)field).getToTable().getName()));
  }


  @Test
  public void queryPathTest() {
    ScriptNode scriptNode = parser.parse("IMPORT ecommerce-data.Product;"
        + "Product.nested := SELECT productid FROM _;"
        + "Product2 := SELECT productid FROM Product.nested;"
    );
    Analysis analysis = analyzer.analyze(scriptNode);
    Generator generator = new Generator(planner, analysis);
    subSchema.setBridge(generator);
    generator.generate(scriptNode);

    Table table = analysis.getSchema().getTable(Name.system("Product2")).get();
    Assertions.assertNotNull(generator.getTable(table.getId()));
  }

  @Test
  public void fullTest() {

    String query = "IMPORT ecommerce-data.Customer;\n"
        + "IMPORT ecommerce-data.Product;\n"
        + "IMPORT ecommerce-data.Orders;\n"
        + "\n"
        + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
        + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
        + "\n"
        + "-- Compute useful statistics on orders\n"
        + "Orders.entries.discount := coalesce(discount, 0.0);\n"
        + "Orders.entries.total := quantity * unit_price - discount;\n"
        + "Orders.total := sum(entries.total);\n"
        + "Orders.total_savings := sum(entries.discount);\n"
        + "Orders.total_entries := count(entries);\n"
        + "\n"
        + "\n"
        + "-- Relate Customer to Orders and compute a customer's total order spent\n"
        + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
        + "Customer.total_orders := sum(_.orders.total);\n"
        + "\n"
        + "-- Aggregate all products the customer has ordered for the 'order again' feature\n"
        + "Orders.entries.product := JOIN Product ON Product.productid = _.productid;\n"
        + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n"
        + "\n"
        + "Customer.recent_products := SELECT productid, e.product.category AS category,\n"
        + "                                   sum(quantity) AS quantity, count(*) AS num_orders\n"
        + "                            FROM _.orders.entries e\n"
        + "                            WHERE parent.time > now() - INTERVAL 2 YEAR\n"
        + "                            GROUP BY productid, category ORDER BY num_orders DESC, "
        + "quantity DESC;\n"
        + "\n"
        + "Customer.recent_products_categories :=\n"
        + "                     SELECT category, count(*) AS num_products\n"
        + "                     FROM _.recent_products\n"
        + "                     GROUP BY category ORDER BY num_products;\n"
        + "\n"
        + "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp"
        + ".category=_.category;\n"
        + "\n"
        + "-- Aggregate customer spending by month and product category for the 'spending "
        + "history' feature\n"
        + "Customer._spending_by_month_category :=\n"
        + "                     SELECT time.roundToMonth(parent.time) AS month,\n"
        + "                            e.product.category AS category,\n"
        + "                            sum(total) AS total,\n"
        + "                            sum(discount) AS savings\n"
        + "                     FROM _.orders.entries e\n"
        + "                     GROUP BY month, category ORDER BY month DESC;\n"
        + "\n"
        + "Customer.spending_by_month :=\n"
        + "                    SELECT month, sum(total) AS total, sum(savings) AS savings\n"
        + "                    FROM _._spending_by_month_category\n"
        + "                    GROUP BY month ORDER BY month DESC;\n"
        + "Customer.spending_by_month.categories :=\n"
        + "    JOIN _.parent._spending_by_month_category c ON c.month=month;\n"
        + "\n"
        + "/* Compute w/w product sales volume increase average over a month\n"
        + "   These numbers are internal to determine trending products */\n"
        + "Product._sales_last_week := SELECT SUM(e.quantity)\n"
        + "                          FROM _.order_entries e\n"
        + "                          --WHERE e.parent.time > now() - INTERVAL 1 WEEK;\n"
        + "                          WHERE e.parent.time > now() - INTERVAL 7 DAY;\n"
        + "\n"
        + "Product._sales_last_month := SELECT SUM(e.quantity)\n"
        + "                          FROM _.order_entries e\n"
        + "                          --WHERE e.parent.time > now() - INTERVAL 4 WEEK;\n"
        + "                          WHERE e.parent.time > now() - INTERVAL 1 MONTH;\n"
        + "\n"
        + "Product._last_week_increase := _sales_last_week * 4 / _sales_last_month;\n"
        + "\n"
        + "-- Determine trending products for each category\n"
        + "Category := SELECT DISTINCT category AS name FROM Product;\n"
        + "Category.products := JOIN Product ON _.name = Product.category;\n"
        + "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase >"
        + " 0\n"
        + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n"
        + "\n"
        + "/* Determine customers favorite categories by total spent\n"
        + "   In combination with trending products this is used for the product recommendation "
        + "feature */\n"
        + "Customer.favorite_categories := SELECT s.category as category_name,\n"
        + "                                        sum(s.total) AS total\n"
        + "                                FROM _._spending_by_month_category s\n"
        + "                                WHERE s.month >= now() - INTERVAL 1 YEAR\n"
        + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n"
        + "\n"
        + "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category"
        + ".name;\n"
        + "\n"
        + "-- Create subscription for customer spending more than $100 so we can send them a "
        + "coupon --\n"
        + "\n"
        + "CREATE SUBSCRIPTION NewCustomerPromotion ON ADD AS\n"
        + "SELECT customerid, email, name, total_orders FROM Customer WHERE total_orders >= 100;\n";

    ScriptNode scriptNode = parser.parse(query);
    Analysis analysis = analyzer.analyze(scriptNode);
    Generator generator = new Generator(planner, analysis);
    subSchema.setBridge(generator);
    generator.generate(scriptNode);
  }
}