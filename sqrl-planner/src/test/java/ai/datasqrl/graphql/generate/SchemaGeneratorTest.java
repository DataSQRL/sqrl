package ai.datasqrl.graphql.generate;

import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.Retail;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;

@Slf4j
public class SchemaGeneratorTest extends AbstractSchemaGeneratorTest {

  private TestDataset example = Retail.INSTANCE;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
    super.setup(testInfo);
  }

  @Test
  public void testImport() {
    snapshotTest("IMPORT ecommerce-data.Product;");
  }

  @Test
  public void testImportNested() {
    snapshotTest("IMPORT ecommerce-data.Orders;");
  }

}