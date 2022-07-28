package ai.datasqrl.sqrl2sql;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.SqrlStatement;
import ai.datasqrl.plan.local.generate.GeneratorBuilder;
import ai.datasqrl.plan.local.generate.Generator;
import ai.datasqrl.util.data.C360;
import java.io.IOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class LogicalPlanRewritingTest extends AbstractSQRLIT {
  private ConfiguredSqrlParser parser;
  private ErrorCollector errorCollector;
  private ImportManager importManager;
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
    generator = GeneratorBuilder.build(importManager, errorCollector);
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
  @Disabled
  public void testSimpleTemporalJoin() {
    runScript(
            "IMPORT ecommerce-data.Orders;\n"
          + "IMPORT ecommerce-data.Product;\n"
          + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
          + "EntryCategories := SELECT e.productid, e.quantity * e.unit_price - e.discount as price, p.name FROM Orders.entries e JOIN Product p ON e.productid = p.productid;\n"
    );
  }

  @Test
  @Disabled
  public void testNestingTemporalJoin() {
    //This currently fails because an ON condition is missing
    runScript(
            "IMPORT ecommerce-data.Orders;\n"
          + "IMPORT ecommerce-data.Product;\n"
          + "Product := DISTINCT Product ON productid ORDER BY _ingest_time DESC;\n"
          + "EntryCategories := SELECT o.id, o.time, e.productid, e.quantity, p.name FROM Orders o JOIN o.entries e JOIN Product p ON e.productid = p.productid;\n"
    );
  }

  public void runScript(String script) {
    ScriptNode node = parser.parse(script);

    for (Node n : node.getStatements()) {
      generator.generate((SqrlStatement)n);
    }
  }
}