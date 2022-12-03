package com.datasqrl.graphql.generate;

import com.datasqrl.AbstractLogicalSQRLIT;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.local.generate.Resolve.Env;
import com.datasqrl.util.SnapshotTest.Snapshot;
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
    SqrlParser parser = new SqrlParser();
    Env env = resolve.planDag(session, parser.parse(sqrlScript));

    GraphQLSchema schema = SchemaGenerator.generate(env.getRelSchema());

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