package ai.datasqrl.graphql.inference;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.graphql.generate.SchemaGenerator;
import ai.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.util.data.Retail;
import ai.datasqrl.util.data.Retail.RetailScriptNames;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaInferenceModelTest extends AbstractLogicalSQRLIT {
  private Retail example = Retail.INSTANCE;

  @BeforeEach
  public void setup() throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), example.getRootPackageDirectory());
  }

  private Env compile(String str) {
    return resolve.planDag(session, ConfiguredSqrlParser.newParser(ErrorCollector.root()).parse(str));
  }

  @SneakyThrows
  @Test
  public void testC360Inference() {
    Path schema = Path.of("src/test/resources/c360bundle/schema.full.graphqls");
    String schemaStr = new String(Files.readAllBytes(schema));
    Env env = compile(Files.readString(example.getScript(RetailScriptNames.FULL).getScript()));

    GraphQLSchema schema1 = SchemaGenerator.generate(env);

    SchemaInference inference = new SchemaInference(schemaStr, env.getRelSchema(), env.getSession().getPlanner()
        .getRelBuilder(), env.getSession().getPlanner());
    InferredSchema s = inference.accept();

    assertEquals(64, s.getQuery().getFields().size());
  }
}