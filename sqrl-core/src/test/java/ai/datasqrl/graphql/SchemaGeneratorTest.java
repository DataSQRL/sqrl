package ai.datasqrl.graphql;

import static ai.datasqrl.util.data.C360.RETAIL_DIR_BASE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.graphql.generate.SchemaGenerator;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.data.C360;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class SchemaGeneratorTest extends AbstractSQRLIT {

  ConfiguredSqrlParser parser;

  ErrorCollector error;

  private Session session;
  private Resolve resolve;
  private TestInfo testInfo;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    error = ErrorCollector.root();
    initialize(IntegrationTestSettings.getInMemory(false));
    C360 example = C360.BASIC;
    example.registerSource(env);

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());
    ScriptBundle bundle = example.buildBundle().getBundle();
    Assertions.assertTrue(
        importManager.registerUserSchema(bundle.getMainScript().getSchema(), error));
    Planner planner = new PlannerFactory(
        CalciteSchema.createRootSchema(false, false).plus()).createPlanner();
    Session session = new Session(error, importManager, planner);
    this.session = session;
    this.parser = new ConfiguredSqrlParser(error);
    this.resolve = new Resolve(RETAIL_DIR_BASE);
    this.testInfo = testInfo;
  }


  @Test
  public void testImport() {
    snapshotTest("IMPORT ecommerce-data.Product;");
  }

  @Test
  public void testImportNested() {
    snapshotTest("IMPORT ecommerce-data.Orders;");
  }

  private void snapshotTest(String sqrl) {
    Env env1 = resolve.planDag(session, parser.parse(sqrl));
    GraphQLSchema schema = SchemaGenerator.generate(env1);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);
    String schemaStr = new SchemaPrinter(opts).print(schema);

    createOrValidateSnapshot(getClass().getName(), testInfo.getDisplayName(), schemaStr);
  }

  @SneakyThrows
  private void createOrValidateSnapshot(String className, String displayName, String content) {
    String snapLocation = String.format("src/test/resources/snapshots/%s/%s.txt",
        className.replace('.', '/'),
        displayName);
    File file = new File(snapLocation);
    if (!file.exists()) {
      file.getParentFile().mkdirs();
      log.info("Test not running, creating snapshot");
      Files.write(file.toPath(), content.getBytes());
      fail("Creating snapshots");
    } else {
      byte[] data = Files.readAllBytes(file.toPath());
      String dataStr = new String(data);
      assertEquals(dataStr, content);
    }
  }
}