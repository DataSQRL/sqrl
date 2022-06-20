package ai.datasqrl.sqrl2sql;

import ai.datasqrl.AbstractSQRLIntegrationTest;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.plan.calcite.CalciteEnvironment;
import ai.datasqrl.plan.calcite.PlanDag;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.SqrlSchemaCatalog;
import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.plan.local.SchemaUpdatePlanner;
import ai.datasqrl.plan.local.operations.SchemaBuilder;
import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.util.data.C360;

import java.io.IOException;
import java.util.Optional;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.BridgedCalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;

class SchemaTest extends AbstractSQRLIntegrationTest {
  ConfiguredSqrlParser parser;
  ErrorCollector errorCollector;
  ImportManager importManager;
  CalciteEnvironment calciteEnv;

  @BeforeEach
  public void setup() throws IOException {
    errorCollector = ErrorCollector.root();
    initialize(IntegrationTestSettings.getDefault(false));
    C360 example = C360.INSTANCE;

    example.registerSource(env);

    importManager = sqrlSettings.getImportManagerProvider().createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().setIncludeSchema(true).getBundle();
    assertTrue(importManager.registerUserSchema(bundle.getMainScript().getSchema(),ErrorCollector.root()));
    calciteEnv = new CalciteEnvironment();
    parser = ConfiguredSqrlParser.newParser(errorCollector);
  }
//
//
//  @SneakyThrows
//  @Test
//  public void testProduct() {
//    ScriptNode node = parser.parse("IMPORT ecommerce-data.Product;");
//    BundleTableFactory tableFactory = new BundleTableFactory(calciteEnv);
//    SchemaBuilder schema = new SchemaBuilder();
//    for (Node n : node.getStatements()) {
//      SchemaUpdatePlanner schemaUpdatePlanner = new SchemaUpdatePlanner(this.importManager,
//          tableFactory, SchemaAdjustmentSettings.DEFAULT,
//          errorCollector, new LocalPlanner(calciteEnv, schema.peek()));
//      System.out.println("Statement: " + NodeFormatter.accept(n));
//
//      Optional<SchemaUpdateOp> op =
//          schemaUpdatePlanner.plan(schema.getSchema(), n);
//      op.ifPresent(o->
//          schema.apply(o));
//    }
//    System.out.println(schema);
//    RelNode node2 = schema.getSchema().get(0).getHead();
//
//    RelNode relNode = new MultiphaseOptimizer().optimize(node2, SqrlPrograms.testProgram);
//    System.out.println(relNode.explain());
//
//
//
//    Class.forName("org.apache.calcite.jdbc.Driver");
//    Properties info = new Properties();
//    info.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
////    info.put(CalciteConnectionProperty.LEX, "java");
//    Connection connection =
//        DriverManager.getConnection("jdbc:calcite:", info);
//    Hook.PROGRAM.run(Programs.sequence(
//        Programs.of(RuleSets.ofList(new DataSourceEnumeratorRule()))
//    ));
//
//    CalciteConnection calciteConnection =
//        connection.unwrap(CalciteConnection.class);
//    System.out.println(calciteConnection.config().caseSensitive());
//    SchemaPlus rootSchema = calciteConnection.getRootSchema();
//    Schema schema1 = new LocalPlanner(calciteEnv, schema.peek()).getCalcitePlanner().getSqrlSchema();
//    rootSchema.add("uuidscript", schema1);
//
//    System.out.println(schema1);
//    Statement statement = calciteConnection.createStatement();
//    ResultSet resultSet = statement.executeQuery(
//        "select \"productid\", count(*)\n"
//            + "from uuidscript.Product GROUP BY \"productid\"");
//
//    output(resultSet, System.out);
//
//    resultSet.close();
//    statement.close();
//    connection.close();
//
//    EnumerableBindable bind = (EnumerableBindable)relNode;
//
//    for (Object[] o : bind.bind(new DataContext() {
//      @Override
//      public SchemaPlus getRootSchema() {
//        return new LocalPlanner(calciteEnv, schema.peek()).getCalcitePlanner().getSchema()
//            .plus();
//      }
//
//      @Override
//      public JavaTypeFactory getTypeFactory() {
//        return new JavaTypeFactoryImpl();
//      }
//
//      @Override
//      public QueryProvider getQueryProvider() {
//        return null;
//      }
//
//      @Override
//      public Object get(String s) {
//        return null;
//      }
//    })) {
//      System.out.println(Arrays.toString(o));
//    }
//  }
//  private void output(ResultSet resultSet, PrintStream out)
//      throws SQLException {
//    final ResultSetMetaData metaData = resultSet.getMetaData();
//    final int columnCount = metaData.getColumnCount();
//    while (resultSet.next()) {
//      for (int i = 1;; i++) {
//        out.print(resultSet.getObject(i));
//        if (i < columnCount) {
//          out.print(", ");
//        } else {
//          out.println();
//          break;
//        }
//      }
//    }
//  }
  @Test
  public void testImport() {
    runScript(
            "IMPORT ecommerce-data.Orders;\n"
    );
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
//         + "Orders.total_entries := count(entries);\n"
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
  public void testNestedPushdown() {
    runScript("IMPORT ecommerce-data.Orders TIMESTAMP uuid AS uuid;\n"
            + "\n"
            + "Orders.entries.discount := coalesce(discount, 0.0);\n"
            + "Orders.entries.total := quantity * unit_price - discount;\n"
            + "Orders.total := sum(entries.total);\n"
            + "Orders.total_savings := sum(entries.discount);\n");
//            + "Orders.total_entries := count(entries);\n");
  }

  public void runScript(String script) {
    ScriptNode node = parser.parse(script);
    BundleTableFactory tableFactory = new BundleTableFactory(calciteEnv);
    SchemaBuilder schema = new SchemaBuilder();

    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    SqrlSchemaCatalog catalog = new SqrlSchemaCatalog(rootSchema);

    String schemaName = "test";
    BridgedCalciteSchema subSchema = new BridgedCalciteSchema();
    catalog.add(schemaName, subSchema);

    PlannerFactory plannerFactory = new PlannerFactory(catalog);
    Planner planner = plannerFactory.createPlanner(schemaName);

    PlanDag dag = new PlanDag(planner);
    subSchema.setBridge(dag);

    for (Node n : node.getStatements()) {
      SchemaUpdatePlanner schemaUpdatePlanner = new SchemaUpdatePlanner(this.importManager,
          tableFactory, SchemaAdjustmentSettings.DEFAULT,
          errorCollector);
      System.out.println("Statement: " + NodeFormatter.accept(n));

      /*
       * Import process flow:
       *  Script or Dataset import def will import a set of tables to be merged into the schema.
       */
      Optional<SchemaUpdateOp> op =
          schemaUpdatePlanner.plan(schema.getSchema(), n);
      op.ifPresent(o->
          schema.apply(o));
      op.ifPresent(o->
          dag.apply(o));
    }
    System.out.println(schema);
  }

}