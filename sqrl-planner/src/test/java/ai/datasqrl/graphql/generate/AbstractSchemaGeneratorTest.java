package ai.datasqrl.graphql.generate;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.util.SnapshotTest.Snapshot;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;

@Slf4j
public class AbstractSchemaGeneratorTest extends AbstractLogicalSQRLIT {

  private Snapshot snapshot;

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    this.snapshot = Snapshot.of(getClass(),testInfo);
  }

  protected String generateSchema(String sqrlScript) {
    ConfiguredSqrlParser parser = new ConfiguredSqrlParser(ErrorCollector.root());
    Env env = resolve.planDag(session, parser.parse(sqrlScript));

    GraphQLSchema schema = SchemaGenerator.generate(env);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
            .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
            .includeDirectives(false);
    String schemaStr = new SchemaPrinter(opts).print(schema);
    return schemaStr;
  }

  protected void snapshotTest(String sqrlScript) {
    snapshot.addContent(generateSchema(sqrlScript));
    snapshot.createOrValidate();
  }
}