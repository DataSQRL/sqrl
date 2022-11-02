package ai.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.datasqrl.AbstractSQRLIT;
import ai.datasqrl.IntegrationTestSettings;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.ScriptBundle.Config;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.graphql.inference.SchemaInference;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class SchemaAnalyzerTest extends AbstractSQRLIT {
  Session session;
  ConfiguredSqrlParser parser;
  Path DIR_BASE = Path.of("../sqml-examples/starwars/starwars");
  private ScriptBundle bundle;

  @BeforeEach
  public void before() {
    initialize(IntegrationTestSettings.getInMemory(false));
    ErrorCollector errors = ErrorCollector.root();
    ScriptBundle bundle = createStarwarsBundle();

    ImportManager importManager = sqrlSettings.getImportManagerProvider()
        .createImportManager(env.getDatasetRegistry());

    DirectorySourceImplementation i = DirectorySourceImplementation.builder()
        .uri(DIR_BASE.toUri().toString())
        .build();

    env.getDatasetRegistry()
        .addOrUpdateSource("starwars", i, errors);
    importManager.registerUserSchema(bundle.getMainScript().getSchema(), errors);

    assertFalse(errors.isFatal(),errors.toString());

    Planner planner = new PlannerFactory(
        CalciteSchema.createRootSchema(false, false).plus()).createPlanner();
    parser = new ConfiguredSqrlParser(errors);
    session = new Session(errors, importManager, planner);

    this.bundle = bundle;
  }

  @SneakyThrows
  @Test
  @Disabled
  public void test() {
    Resolve resolve = new Resolve(DIR_BASE);
    ScriptNode node = parser.parse(bundle.getMainScript().getContent());
    Env env2 = resolve.planDag(session, node);

    String gql = Files.readString(Path.of("../sqml-examples/starwars")
        .resolve("starwars.graphql"));

    TypeDefinitionRegistry typeDefinitionRegistry =
        (new SchemaParser()).parse(gql);

    SchemaInference inference = new SchemaInference();
    Root root = inference.visitTypeDefinitionRegistry(typeDefinitionRegistry, env2);
    assertNotNull(root);
  }

  @SneakyThrows
  ScriptBundle createStarwarsBundle() {
    Path preschema = DIR_BASE.resolve("pre-schema.yml");
    Path sqrl = DIR_BASE.resolve("starwars.sqrl");

    ScriptBundle.Config config = new Config("starwars",
        "1",
        List.of(new SqrlScript.Config("starwars", null, Files.readString(sqrl),
            Files.readString(preschema), true)),
        List.of());
    ErrorCollector errors = ErrorCollector.root();
    ScriptBundle bundle = config.initialize(errors);
    assertFalse(errors.isFatal(),errors.toString());
    return bundle;
  }
}