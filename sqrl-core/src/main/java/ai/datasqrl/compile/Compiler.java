package ai.datasqrl.compile;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.graphql.GraphQLServer;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.PhysicalPlanExecutor;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.util.db.JDBCTempDatabase;
import io.vertx.core.Vertx;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import ai.datasqrl.graphql.generate.SchemaGenerator;
import ai.datasqrl.graphql.inference.SchemaInference;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.queries.APIQuery;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphqlTypeComparatorRegistry;
import graphql.schema.idl.SchemaPrinter;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;

public class Compiler {

  public static void main(String[] args) {
    Path build = Path.of(".").resolve("build");

    Compiler compiler = new Compiler();
    compiler.run(build);
  }

  /**
   * Process: All the files are in the build directory
   */
  @SneakyThrows
  public void run(Path build) {


    ErrorCollector collector = ErrorCollector.root();
    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());
    Planner planner = new PlannerFactory(schema.plus()).createPlanner();

    Session s = new Session(collector, new ImportManager(null), planner);

    Resolve resolve = new Resolve(build);

    File file = new File(build
        .resolve("main.sqrl").toUri());
    String str = Files.readString(file.toPath());

    ScriptNode ast = ConfiguredSqrlParser.newParser(collector)
        .parse(str);

    Env env = resolve.planDag(s, ast);

    //TODO: push compute to the api
    Root root = writeGraphql(env, build);

    JDBCTempDatabase jdbcTempDatabase = new JDBCTempDatabase();
    PhysicalPlan plan = dryRunFlink(env, s, jdbcTempDatabase);
    exec(plan, jdbcTempDatabase);
    startGraphql(build, root, jdbcTempDatabase);
  }

  @SneakyThrows
  private void startGraphql(Path build, Root root, JDBCTempDatabase jdbcTempDatabase) {
    CompletableFuture future = Vertx.vertx().deployVerticle(new GraphQLServer(
        build,
        root, jdbcTempDatabase))
        .toCompletionStage()
        .toCompletableFuture()
        ;
    future.get();
//    Launcher.executeCommand("run", GraphQLServer.class.getName()
//        /*, "--conf", "config.json"*/);
  }

  @SneakyThrows
  public HttpResponse<String> testQuery(String query) {
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

  private void exec(PhysicalPlan plan, JDBCTempDatabase jdbcTempDatabase) {
    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
    Job job = executor.execute(plan, Optional.of(jdbcTempDatabase));
    System.out.println("Started Flink Job: " + job.getExecutionId());
  }

  private PhysicalPlan dryRunFlink(Env env, Session s, JDBCTempDatabase jdbcTempDatabase) {
    DAGPlanner dagPlanner = new DAGPlanner(s.getPlanner());
    //We add a scan query for every query table
    List<APIQuery> queries = new ArrayList<APIQuery>();
    CalciteSchema relSchema = env.getRelSchema();
    addAllQueryTables(env.getSession().getPlanner(), relSchema, queries);
    OptimizedDAG dag = dagPlanner.plan(relSchema,queries);
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(s.getImportManager(),
        jdbcTempDatabase.getJdbcConfiguration().getDatabase("test"),
        new LocalFlinkStreamEngineImpl(), s.getPlanner()
        );
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag, Optional.of(jdbcTempDatabase));
    FlinkStreamPhysicalPlan ph = (FlinkStreamPhysicalPlan)physicalPlan.getStreamQueries();
    //write

    return physicalPlan;
    //just execute for now


  }

  private void addAllQueryTables(Planner planner, CalciteSchema relSchema, List<APIQuery> queries) {
    relSchema.getTableNames().stream()
        .map(t->relSchema.getTable(t, false))
        .filter(t->t.getTable() instanceof VirtualRelationalTable)
        .forEach(vt->{
          RelNode rel = planner.getRelBuilder().scan(vt.name).build();

          queries.add(new APIQuery(vt.name.substring(0,vt.name.indexOf(Name.NAME_DELIMITER)), rel));
        });

  }

  @SneakyThrows
  private Root writeGraphql(Env env, Path build) {
    GraphQLSchema schema = SchemaGenerator.generate(env);

    SchemaPrinter.Options opts = SchemaPrinter.Options.defaultOptions()
        .setComparators(GraphqlTypeComparatorRegistry.AS_IS_REGISTRY)
        .includeDirectives(false);
    String schemaStr = new SchemaPrinter(opts).print(schema);

    SchemaInference inference = new SchemaInference();
    Root root = inference.visitSchema(schemaStr, env);

    ObjectMapper mapper = new ObjectMapper();

    Path filePath = build.resolve("api");
    File file = filePath.toFile();
    file.mkdirs();

    try {
      file.toPath().resolve("plan.json").toFile().delete();
      //Write content to file
      Files.writeString(file.toPath().resolve("plan.json"),
          mapper.writeValueAsString(root), StandardOpenOption.CREATE);
      System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
      return root;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return root;
  }
}
