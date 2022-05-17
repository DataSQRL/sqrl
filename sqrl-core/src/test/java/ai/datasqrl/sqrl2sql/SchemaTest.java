package ai.datasqrl.sqrl2sql;

import ai.datasqrl.C360Example;
import ai.datasqrl.Environment;
import ai.datasqrl.api.ConfigurationTest;
import ai.datasqrl.config.SqrlSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.parse.SqrlParser;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.plan.local.LocalPlanner2;
import ai.datasqrl.plan.local.operations.SchemaBuilder;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.server.ImportManager;
import ai.datasqrl.plan.local.SchemaUpdatePlanner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaTest {
  SqrlParser parser;
  ErrorCollector errorCollector;
  ImportManager importManager;

  @BeforeEach
  public void setup() throws IOException {
    errorCollector = ErrorCollector.root();

    SqrlSettings settings = ConfigurationTest.getDefaultSettings(false);
    Environment env = Environment.create(settings);

    String ds2Name = "ecommerce-data";
    DirectorySourceImplementation fileConfig = DirectorySourceImplementation.builder()
        .uri(C360Example.RETAIL_DATA_DIR.toAbsolutePath().toString())
        .build();
    env.getDatasetRegistry().addOrUpdateSource(ds2Name, fileConfig, ErrorCollector.root());

    importManager = settings.getImportManagerProvider().createImportManager(env.getDatasetRegistry());
    ScriptBundle.Config config = ScriptBundle.Config.builder()
        .name(C360Example.RETAIL_SCRIPT_NAME)
        .scripts(ImmutableList.of(
            SqrlScript.Config.builder()
                .name(C360Example.RETAIL_SCRIPT_NAME)
                .main(true)
                .content("IMPORT ecommerce-data.Orders;")
                .inputSchema(Files.readString(C360Example.RETAIL_IMPORT_SCHEMA_FILE))
                .build()
        ))
        .build();
    ScriptBundle bundle = config.initialize(errorCollector);
    System.out.println(errorCollector);
    importManager.registerUserSchema(bundle.getMainScript().getSchema());

    parser = SqrlParser.newParser(errorCollector);
  }

  @Test
  public void testExpression() {
     runScript(
         "IMPORT ecommerce-data.Orders;\n"
         + "Orders.entries.discount := coalesce(discount, 0.0);\n");
  }


  @Test
  public void testExpression2() {
    runScript(
        "IMPORT ecommerce-data.Orders;\n"
            + "Orders.entries.discount := coalesce(discount, 0.0);\n"
            + "Orders.entries.total := quantity * unit_price - discount;");
  }


  @Test
  public void testQuery() {
     runScript("IMPORT ecommerce-data.Customer;\n"
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
         + "-- Relate Customer to Orders and compute a customer's total order spent\n"
         + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
         + "Customer.total_orders := sum(orders.total);\n"
         + "\n"
         + "-- Aggregate all products the customer has ordered for the 'order again' feature\n"
         + "Orders.entries.product := LEFT JOIN Product ON Product.productid = _.productid LIMIT 1;\n"
         + "Product.order_entries := JOIN Orders.entries e ON e.productid = _.productid;\n"
         + "\n"
         + "Customer.recent_products := SELECT productid, product.category AS category,\n"
         + "                                   sum(quantity) AS quantity, count(*) AS num_orders\n"
         + "                            FROM _.orders.entries\n"
         + "                            WHERE parent.time > now() - INTERVAL 2 YEAR\n"
         + "                            GROUP BY productid, category ORDER BY num_orders DESC, quantity DESC;\n"
         + "\n"
         + "Customer.recent_products_categories :=\n"
         + "                     SELECT category, count(*) AS num_products\n"
         + "                     FROM _.recent_products\n"
         + "                     GROUP BY category ORDER BY num_products;\n"
         + "\n"
         + "Customer.recent_products_categories.products := JOIN _.parent.recent_products rp ON rp.category=_.category;\n"
         + "\n"
         + "-- Aggregate customer spending by month and product category for the 'spending history' feature\n"
         + "Customer._spending_by_month_category :=\n"
         + "                     SELECT time.roundToMonth(parent.time) AS month,\n"
         + "                            product.category AS category,\n"
         + "                            sum(total) AS total,\n"
         + "                            sum(discount) AS savings\n"
         + "                     FROM _.orders.entries\n"
         + "                     GROUP BY month, category ORDER BY month DESC;\n"
         + "\n"
         + "Customer.spending_by_month :=\n"
         + "                    SELECT month, sum(total) AS total, sum(savings) AS savings\n"
         + "                    FROM _._spending_by_month_category\n"
         + "                    GROUP BY month ORDER BY month DESC;\n"
         + "Customer.spending_by_month.categories :=\n"
         + "    JOIN _.parent._spending_by_month_category c ON c.month = _.month;\n"
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
         + "Category.trending := JOIN Product p ON _.name = p.category AND p._last_week_increase > 0\n"
         + "                     ORDER BY p._last_week_increase DESC LIMIT 10;\n"
         + "\n"
         + "/* Determine customers favorite categories by total spent\n"
         + "   In combination with trending products this is used for the product recommendation feature */\n"
         + "Customer.favorite_categories := SELECT s.category as category_name,\n"
         + "                                        sum(s.total) AS total\n"
         + "                                FROM _._spending_by_month_category s\n"
         + "                                WHERE s.month >= now() - INTERVAL 1 YEAR\n"
         + "                                GROUP BY category_name ORDER BY total DESC LIMIT 5;\n"
         + "\n"
         + "Customer.favorite_categories.category := JOIN Category ON _.category_name = Category.name;\n"
         + "\n"
         + "-- Create subscription for customer spending more than $100 so we can send them a coupon --\n"
         + "\n"
         + "CREATE SUBSCRIPTION NewCustomerPromotion ON ADD AS\n"
         + "SELECT customerid, email, name, total_orders FROM Customer WHERE total_orders >= 100;\n");
  }

  @Test
  public void testQuerySmall() {
    runScript("IMPORT ecommerce-data.Customer;\n"
            + "IMPORT ecommerce-data.Orders;\n"
            + "\n"
            + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
            + "\n"
            + "-- Relate Customer to Orders and compute a customer's total order spent\n"
            + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
            + "Customer.total_count := sum(orders.entries.quantity);\n");
  }

  @Test
  public void testQueryExperiment() {
    runScript("IMPORT ecommerce-data.Customer;\n"
            + "IMPORT ecommerce-data.Orders;\n"
            + "\n"
            + "Customer := DISTINCT Customer ON customerid ORDER BY _ingest_time DESC;\n"
            + "\n"
            + "-- Relate Customer to Orders and compute a customer's total order spent\n"
            + "Customer.orders := JOIN Orders ON Orders.customerid = _.customerid;\n"
            + "Customer.count := SELECT SUM(e.quantity) as total, AVG(e.quantity) as average FROM _.orders.entries e;\n"
            + "CustomerCount := SELECT c.customerid, SUM(e.quantity) as total, AVG(e.quantity) as average FROM Customer c JOIN c.orders.entries e GROUP BY c.customerid;\n"
    );
  }

    public void runScript(String script) {
    ScriptNode node = parser.parse(script);
    SchemaBuilder schema = new SchemaBuilder();

    for (Node n : node.getStatements()) {
      SchemaUpdatePlanner schemaUpdatePlanner = new SchemaUpdatePlanner(this.importManager,
          errorCollector, new LocalPlanner2(schema.peek()));
      System.out.println("Statement: " + NodeFormatter.accept(n));

      /*
       * Import process flow:
       *  Script or Dataset import def will import a set of tables to be merged into the schema.
       */
      Optional<SchemaUpdateOp> op =
          schemaUpdatePlanner.plan(schema.getSchema(), n);
      op.ifPresent(o->
          schema.apply(o));
    }
    System.out.println(schema);
  }

}