package ai.datasqrl.graphql.generate;

import static org.junit.jupiter.api.Assertions.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.SnapshotTest;
import ai.datasqrl.util.SnapshotTest.Snapshot;
import ai.datasqrl.util.TestDataset;
import ai.datasqrl.util.data.C360;
import ai.datasqrl.util.data.Retail;
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
public class SchemaGeneratorTest extends AbstractLogicalSQRLIT {

  private TestDataset example = Retail.INSTANCE;
  private Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
    this.snapshot = SnapshotTest.Snapshot.of(getClass(),testInfo);
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
    ConfiguredSqrlParser parser = new ConfiguredSqrlParser(ErrorCollector.root());
    Env env = resolve.planDag(session, parser.parse(sqrl));

    GraphQLSchema schema = SchemaGenerator.generate(env);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);
    String schemaStr = new SchemaPrinter(opts).print(schema);

    snapshot.addContent(schemaStr);
    snapshot.createOrValidate();
  }
}