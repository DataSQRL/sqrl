package ai.datasqrl.graphql;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.graphql.generate.SchemaGenerator;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.Retail;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@Disabled
public class SchemaGeneratorTest extends AbstractSQRLIT {

  private TestInfo testInfo;
  private TestDataset example = Retail.INSTANCE;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
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
  public static void createOrValidateSnapshot(String className, String displayName, String content) {
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