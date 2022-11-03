package ai.datasqrl.graphql;

import ai.datasqrl.AbstractLogicalSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.graphql.inference.SchemaInference;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import lombok.SneakyThrows;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Disabled
class SchemaAnalyzerTest extends AbstractLogicalSQRLIT {

  Path DIR_BASE = Path.of("../sqml-examples/starwars/starwars");

  @BeforeEach
  public void before() {
    initialize(IntegrationTestSettings.getInMemory(), DIR_BASE);
  }

  @SneakyThrows
  @Test
  @Disabled
  public void test() {
    Resolve resolve = new Resolve(DIR_BASE);
    ScriptNode node = parser.parse("starwars.sqrl");
    Env env2 = resolve.planDag(session, node);

    String gql = Files.readString(Path.of("../sqml-examples/starwars")
        .resolve("starwars.graphql"));

    TypeDefinitionRegistry typeDefinitionRegistry =
        (new SchemaParser()).parse(gql);

    SchemaInference inference = new SchemaInference();
    Root root = inference.visitTypeDefinitionRegistry(typeDefinitionRegistry, env2);
    assertNotNull(root);
  }
}