package ai.datasqrl.sqrl2sql;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.data.C360;
import java.io.IOException;

import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogicalPlanRewritingTest extends AbstractSQRLIT {
  private ConfiguredSqrlParser parser;
  private ErrorCollector errorCollector;
  private ImportManager importManager;
  private Resolve resolve;
  private Session session;

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
    this.session = new Session(errorCollector, importManager,
        new PlannerFactory(CalciteSchema.createRootSchema(false, false).plus()).createPlanner());
    this.resolve = new Resolve();
  }

  @Test
  public void testSimpleQuery() {
    runScript(
            "IMPORT ecommerce-data.Orders;\n"
          + "IMPORT ecommerce-data.Product;\n"
          + "IMPORT ecommerce-data.Customer;\n"
          + "EntryCount := SELECT e.quantity * e.unit_price - e.discount as price FROM Orders.entries e;\n"
    );
  }

  @Test
  public void testSimpleTemporalJoin() {
    runScript(
            "IMPORT ecommerce-data.Orders;\n"
          + "IMPORT ecommerce-data.Product;\n"
          + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
          + "EntryCategories := SELECT e.productid, e.quantity * e.unit_price - e.discount as price, p.name "
          + "                   FROM Orders.entries e "
          + "                   JOIN Product p ON e.productid = p.productid;\n"
    );
  }

  @Test
  public void testNestingTemporalJoin() {
    runScript(
            "IMPORT ecommerce-data.Orders;\n"
          + "IMPORT ecommerce-data.Product;\n"
          + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
          + "EntryCategories := SELECT o.id, o.\"time\", e.productid, e.quantity, p.name FROM Orders o JOIN o.entries e JOIN Product p ON e.productid = p.productid;\n"
    );
  }

  public void runScript(String script) {
    ScriptNode node = parser.parse(script);
    resolve.planDag(session, node);
  }
}