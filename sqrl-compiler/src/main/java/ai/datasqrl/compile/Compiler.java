package ai.datasqrl.compile;

import ai.datasqrl.config.CompilerConfiguration;
import ai.datasqrl.config.EngineSettings;
import ai.datasqrl.config.GlobalCompilerConfiguration;
import ai.datasqrl.config.GlobalEngineConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.graphql.generate.SchemaGenerator;
import ai.datasqrl.graphql.inference.PgSchemaBuilder;
import ai.datasqrl.graphql.inference.SchemaInference;
import ai.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import ai.datasqrl.graphql.server.Model.RootGraphqlModel;
import ai.datasqrl.graphql.util.ReplaceGraphqlQueries;
import ai.datasqrl.parse.SqrlParser;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.spi.ManifestConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.sql.ScriptNode;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class Compiler {

  /**
   * Processes all the files in the build directory and creates the execution artifacts
   */
  @SneakyThrows
  public void run(ErrorCollector collector, Path packageFile) {
    Preconditions.checkArgument(Files.isRegularFile(packageFile));
    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());

    Path buildDir = packageFile.getParent();
    GlobalCompilerConfiguration globalConfig = GlobalEngineConfiguration.readFrom(packageFile, GlobalCompilerConfiguration.class);
    CompilerConfiguration config = globalConfig.initializeCompiler(collector);
    EngineSettings engineSettings = globalConfig.initializeEngines(collector);
    Planner planner = new PlannerFactory(schema.plus()).createPlanner();
    Session s = new Session(collector, planner, engineSettings.getPipeline());
    Resolve resolve = new Resolve(buildDir);

    ManifestConfiguration manifest = globalConfig.getManifest();
    Preconditions.checkArgument(manifest!=null);
    Path mainScript = buildDir.resolve(manifest.getMain());
    Optional<Path> graphqlSchema = manifest.getOptGraphQL().map(file -> buildDir.resolve(file));

    String str = Files.readString(mainScript);

    ScriptNode ast = SqrlParser.newParser()
        .parse(str);

    Env env = resolve.planDag(s, ast);

    String gqlSchema = inferOrGetSchema(env, graphqlSchema);

    InferredSchema inferredSchema = new SchemaInference(gqlSchema, env.getRelSchema(),
        env.getSession().getPlanner().getRelBuilder())
        .accept();

    PgSchemaBuilder pgSchemaBuilder = new PgSchemaBuilder(gqlSchema,
        env.getRelSchema(),
        env.getSession().getPlanner().getRelBuilder(),
        env.getSession().getPlanner());

    RootGraphqlModel root = inferredSchema.accept(pgSchemaBuilder, null);

    OptimizedDAG dag = optimizeDag(pgSchemaBuilder.getApiQueries(), env);
    PhysicalPlan plan = createPhysicalPlan(dag, env, s);

    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    log.info("build dir: " + buildDir.toAbsolutePath());
    writeGraphql(buildDir, root, gqlSchema);
  }

  private OptimizedDAG optimizeDag(List<APIQuery> queries, Env env) {
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner(),
        env.getSession().getPipeline());
    CalciteSchema relSchema = env.getRelSchema();
    return dagPlanner.plan(relSchema, queries, env.getExports());
  }

  private RootGraphqlModel updateGraphqlPlan(RootGraphqlModel root, Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  private PhysicalPlan createPhysicalPlan(OptimizedDAG dag, Env env, Session s) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(s.getPlanner().getRelBuilder());
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;
  }

  @SneakyThrows
  public String inferOrGetSchema(Env env, Optional<Path> graphqlSchema) {
    if (graphqlSchema.map(s -> s.toFile().exists()).orElse(false)) {
      return Files.readString(graphqlSchema.get());
    }
    GraphQLSchema schema = SchemaGenerator.generate(env.getRelSchema());

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);
    String schemaStr = new SchemaPrinter(opts).print(schema);

    return schemaStr;
  }

  @SneakyThrows
  private void writeGraphql(Path build, RootGraphqlModel root, String gqlSchema) {
    ObjectMapper mapper = new ObjectMapper();

    Path filePath = build.resolve("api");
    File file = filePath.toFile();
    file.mkdirs();

    try {
      file.toPath().resolve("plan.json").toFile().delete();
      file.toPath().resolve("schema.graphqls").toFile().delete();

      //Write content to file
      Files.writeString(file.toPath().resolve("plan.json"),
          mapper.writeValueAsString(root), StandardOpenOption.CREATE);
      Files.writeString(file.toPath().resolve("schema.graphqls"),
          gqlSchema, StandardOpenOption.CREATE);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
