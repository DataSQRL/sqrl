package ai.datasqrl.compile;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.graphql.GraphQLServer;
import ai.datasqrl.graphql.generate.SchemaGenerator;
import ai.datasqrl.graphql.inference.PgBuilder;
import ai.datasqrl.graphql.inference.SchemaInference;
import ai.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.util.ReplaceGraphqlQueries;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.physical.database.relational.QueryTemplate;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.PhysicalPlanExecutor;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.plan.queries.APIQuery;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import io.vertx.core.Vertx;
import java.io.File;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.sql.ScriptNode;

@Slf4j
public class Compiler {

  public static void main(String[] args) {
    Path build = Path.of(".").resolve("build");

    Compiler compiler = new Compiler();
    compiler.run(build, Optional.of(build.resolve("schema.graphqls")), Optional.empty());
  }

  /**
   * Process: All the files are in the build directory
   */
  @SneakyThrows
  public void run(Path build, Optional<Path> graphqlSchema, Optional<JDBCConnectionProvider> jdbcConf) {
    ErrorCollector collector = ErrorCollector.root();
    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());
    Planner planner = new PlannerFactory(schema.plus()).createPlanner();

    Session s = new Session(collector, planner);

    Resolve resolve = new Resolve(build);

    File file = new File(build
        .resolve("main.sqrl").toUri());
    String str = Files.readString(file.toPath());

    ScriptNode ast = ConfiguredSqrlParser.newParser(ErrorCollector.root())
        .parse(str);

    Env env = resolve.planDag(s, ast);

    //TODO: push compute to the api
    String gqlSchema = inferOrGetSchema(env, build, graphqlSchema);

    SchemaInference inference2 = new SchemaInference(gqlSchema,
        env.getRelSchema(), env.getSession().getPlanner().getRelBuilder()
    );
    InferredSchema inferredSchema = inference2.accept();
    PgBuilder pgBuilder = new PgBuilder(gqlSchema,
        env.getRelSchema(),
        env.getSession().getPlanner().getRelBuilder(),
        env.getSession().getPlanner() );

    Root root = inferredSchema.accept(pgBuilder, null);

    OptimizedDAG dag = optimizeDag(pgBuilder.getApiQueries(), env);

    PhysicalPlan plan = createPhysicalPlan(dag, env, s, jdbcConf.get());
    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    log.info("PORT: " + jdbcConf.get().getPort());
    log.info("build dir: " + build.toAbsolutePath().toString());
    writeGraphql(env, build, root, gqlSchema);
    exec(plan);

    startGraphql(build, root, jdbcConf.get());
  }

  private OptimizedDAG optimizeDag(List<APIQuery> queries, Env env) {
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner());
    //We add a scan query for every query table
    CalciteSchema relSchema = env.getRelSchema();
    //todo comment out
//    addAllQueryTables(env.getSession().getPlanner(), relSchema, queries);
    OptimizedDAG dag = dagPlanner.plan(relSchema,queries, env.getExports(), env.getSession().getPipeline());

    return dag;
  }

  private Root updateGraphqlPlan(Root root, Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  private PhysicalPlan createPhysicalPlan(OptimizedDAG dag, Env env, Session s,
      JDBCConnectionProvider jdbcConf) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(
        jdbcConf,
//        jdbcConf.getDatabase("test"),
        new LocalFlinkStreamEngineImpl(), s.getPlanner()
    );
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag);
    return physicalPlan;

  }

  @SneakyThrows
  public static void startGraphql(Path build, Root root, JDBCConnectionProvider jdbcConf) {
    CompletableFuture future = Vertx.vertx().deployVerticle(new GraphQLServer(
            root, jdbcConf))
        .toCompletionStage()
        .toCompletableFuture()
        ;
    future.get();
//    Launcher.executeCommand("run", GraphQLServer.class.getName()
//        /*, "--conf", "config.json"*/);
  }

  @SneakyThrows
  public static HttpResponse<String> testQuery(String query) {
    ObjectMapper mapper = new ObjectMapper();
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request = HttpRequest.newBuilder()
        .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(
            Map.of("query", query))))
        .uri(URI.create("http://localhost:8888/graphql"))
        .build();
    return client.send(request, BodyHandlers.ofString());
  }

  public static HttpRequest.BodyPublisher ofFormData(Map<Object, Object> data) {
    var builder = new StringBuilder();
    for (Map.Entry<Object, Object> entry : data.entrySet()) {
      if (builder.length() > 0) {
        builder.append("&");
      }
      builder.append(URLEncoder.encode(entry.getKey().toString(), StandardCharsets.UTF_8));
      builder.append("=");
      builder.append(URLEncoder.encode(entry.getValue().toString(), StandardCharsets.UTF_8));
    }
    return HttpRequest.BodyPublishers.ofString(builder.toString());
  }

  private void exec(PhysicalPlan plan) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    Job job = executor.execute(plan);
    log.trace("Started Flink Job: {}", job.getExecutionId());
  }

  @SneakyThrows
  public String inferOrGetSchema(Env env, Path build, Optional<Path> graphqlSchema) {
    if (graphqlSchema.map(s->s.toFile().exists()).orElse(false)) {
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
  private void writeGraphql(Env env, Path build, Root root, String gqlSchema) {

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
