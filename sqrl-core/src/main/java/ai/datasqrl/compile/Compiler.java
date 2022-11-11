package ai.datasqrl.compile;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.graphql.GraphQLServer;
import ai.datasqrl.graphql.generate.SchemaGenerator;
import ai.datasqrl.graphql.inference.ArgumentSet;
import ai.datasqrl.graphql.inference.PgBuilder;
import ai.datasqrl.graphql.inference.SchemaInference;
import ai.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
import ai.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import ai.datasqrl.graphql.server.Model.ArgumentPgParameter;
import ai.datasqrl.graphql.server.Model.CoordVisitor;
import ai.datasqrl.graphql.server.Model.FieldLookupCoords;
import ai.datasqrl.graphql.server.Model.GraphQLArgumentWrapper;
import ai.datasqrl.graphql.server.Model.GraphQLArgumentWrapperVisitor;
import ai.datasqrl.graphql.server.Model.PagedPgQuery;
import ai.datasqrl.graphql.server.Model.ParameterHandlerVisitor;
import ai.datasqrl.graphql.server.Model.PgQuery;
import ai.datasqrl.graphql.server.Model.QueryBaseVisitor;
import ai.datasqrl.graphql.server.Model.ResolvedPagedPgQuery;
import ai.datasqrl.graphql.server.Model.ResolvedPgQuery;
import ai.datasqrl.graphql.server.Model.ResolvedQueryVisitor;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.server.Model.RootVisitor;
import ai.datasqrl.graphql.server.Model.SchemaVisitor;
import ai.datasqrl.graphql.server.Model.SourcePgParameter;
import ai.datasqrl.graphql.server.Model.StringSchema;
import ai.datasqrl.graphql.server.Model.TypeDefinitionSchema;
import ai.datasqrl.graphql.util.ApiQueryBase;
import ai.datasqrl.graphql.util.ApiQueryVisitor;
import ai.datasqrl.graphql.util.PagedApiQueryBase;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.physical.database.relational.QueryTemplate;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.PhysicalPlanExecutor;
import ai.datasqrl.physical.stream.flink.LocalFlinkStreamEngineImpl;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.schema.builder.VirtualTable;
import ai.datasqrl.util.db.JDBCTempDatabase;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

public class Compiler {

  public static void main(String[] args) {
    Path build = Path.of(".").resolve("build");

    Compiler compiler = new Compiler();
    compiler.run(build, Optional.of(build.resolve("schema.graphqls")));
  }

  /**
   * Process: All the files are in the build directory
   */
  @SneakyThrows
  public void run(Path build, Optional<Path> graphqlSchema) {
    ErrorCollector collector = ErrorCollector.root();
    SqrlCalciteSchema schema = new SqrlCalciteSchema(
        CalciteSchema.createRootSchema(false, false).plus());
    Planner planner = new PlannerFactory(schema.plus()).createPlanner();

    Session s = new Session(collector, planner);

    Resolve resolve = new Resolve(build);

    File file = new File(build
        .resolve("main.sqrl").toUri());
    String str = Files.readString(file.toPath());

    ScriptNode ast = ConfiguredSqrlParser.newParser(collector)
        .parse(str);

    Env env = resolve.planDag(s, ast);

    //TODO: push compute to the api
    String gqlSchema = inferOrGetSchema(env, build, graphqlSchema);

    SchemaInference inference2 = new SchemaInference(gqlSchema,
        env.getRelSchema(), env.getSession().getPlanner().getRelBuilder(),
        env.getSession().getPlanner());
    InferredSchema inferredSchema = inference2.accept();
    PgBuilder pgBuilder = new PgBuilder(gqlSchema,
        env.getRelSchema(),
        env.getSession().getPlanner().getRelBuilder(),
        env.getSession().getPlanner() );

    Root root = inferredSchema.accept(pgBuilder, null);

    OptimizedDAG dag = optimizeDag(pgBuilder.getApiQueries(), env);
    JDBCTempDatabase jdbcTempDatabase = new JDBCTempDatabase();

    PhysicalPlan plan = createPhysicalPlan(dag, env, s, jdbcTempDatabase);
    root = updateGraphqlPlan(root, plan.getDatabaseQueries());

    System.out.println("PORT: " + jdbcTempDatabase.getPostgreSQLContainer().getMappedPort(5432));
    System.out.println("build dir: " + build.toAbsolutePath().toString());
    writeGraphql(env, build, root, gqlSchema);
    exec(plan, jdbcTempDatabase);

    startGraphql(build, root, jdbcTempDatabase);
  }

  private OptimizedDAG optimizeDag(List<APIQuery> queries, Env env) {
    DAGPlanner dagPlanner = new DAGPlanner(env.getSession().getPlanner());
    //We add a scan query for every query table
    CalciteSchema relSchema = env.getRelSchema();
    //todo comment out
//    addAllQueryTables(env.getSession().getPlanner(), relSchema, queries);
    OptimizedDAG dag = dagPlanner.plan(relSchema,queries, env.getSession().getPipeline());

    return dag;
  }

  private List<APIQuery> buildApiQueries(Root root) {


    return null;
  }

  private Root updateGraphqlPlan(Root root, Map<APIQuery, QueryTemplate> queries) {
    ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(queries);
    root.accept(replaceGraphqlQueries, null);
    return root;
  }

  private PhysicalPlan createPhysicalPlan(OptimizedDAG dag, Env env, Session s, JDBCTempDatabase jdbcTempDatabase) {
    PhysicalPlanner physicalPlanner = new PhysicalPlanner(
        jdbcTempDatabase.getJdbcConfiguration().getDatabase("test"),
        new LocalFlinkStreamEngineImpl(), s.getPlanner()
    );
    PhysicalPlan physicalPlan = physicalPlanner.plan(dag, Optional.of(jdbcTempDatabase));
    return physicalPlan;

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

  private void addAllQueryTables(Planner planner, CalciteSchema relSchema, List<APIQuery> queries) {
    relSchema.getTableNames().stream()
        .map(t->relSchema.getTable(t, false))
        .filter(t->t.getTable() instanceof VirtualTable)
        .forEach(vt->{
          RelNode rel = planner.getRelBuilder().scan(vt.name).build();

          queries.add(new APIQuery(vt.name.substring(0,vt.name.indexOf(Name.NAME_DELIMITER)), rel));
        });

  }

  @SneakyThrows
  public String inferOrGetSchema(Env env, Path build, Optional<Path> graphqlSchema) {
    if (graphqlSchema.map(s->s.toFile().exists()).orElse(false)) {
      return Files.readString(graphqlSchema.get());
    }
    GraphQLSchema schema = SchemaGenerator.generate(env);

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
    System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(root));

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

  public class ReplaceGraphqlQueries implements
      RootVisitor<Object, Object>,
      CoordVisitor<Object, Object>,
      SchemaVisitor<Object, Object>,
      GraphQLArgumentWrapperVisitor<Object, Object>,
      QueryBaseVisitor<PgQuery, Object>,
      ApiQueryVisitor<PgQuery, Object>,
      ResolvedQueryVisitor<Object, Object>,
      ParameterHandlerVisitor<Object, Object>
  {

    private final Map<APIQuery, QueryTemplate> queries;

    public ReplaceGraphqlQueries(Map<APIQuery, QueryTemplate> queries) {

      this.queries = queries;
    }

    @Override
    public PgQuery visitApiQuery(ApiQueryBase apiQueryBase, Object context) {
      QueryTemplate template = queries.get(apiQueryBase.getQuery());

      SqlWriterConfig config = RelToSql.transform.apply(SqlPrettyWriter.config());
      DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config);
      String query = convertDynamicParams(writer, template.getRelNode(), apiQueryBase.getRelAndArg());
      return PgQuery.builder()
          .parameters(apiQueryBase.getParameters())
          .sql(query)
          .build();
    }

    @Override
    public PgQuery visitPagedApiQuery(PagedApiQueryBase apiQueryBase, Object context) {
      QueryTemplate template = queries.get(apiQueryBase.getQuery());

      //todo builder
      SqlWriterConfig config = RelToSql.transform.apply(SqlPrettyWriter.config());
      DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config);
      String query = convertDynamicParams(writer, template.getRelNode(), apiQueryBase.getRelAndArg());
      Preconditions.checkState(Collections.max(writer.dynamicParameters) < apiQueryBase.getParameters().size());
      return new PagedPgQuery(
          query,
          apiQueryBase.getParameters());
    }

    private String convertDynamicParams(DynamicParamSqlPrettyWriter writer, RelNode relNode, ArgumentSet arg) {
      SqlNode node = RelToSql.convertToSqlNode(relNode);
      node.unparse(writer, 0, 0);
      return writer.toSqlString().getSql();
    }

    /**
     * Writes postgres style dynamic params `$1` instead of `?`. Assumes the index field is the index
     * of the parameter.
     */
    public class DynamicParamSqlPrettyWriter extends SqlPrettyWriter {

      @Getter
      private List<Integer> dynamicParameters = new ArrayList<>();

      public DynamicParamSqlPrettyWriter(@NotNull SqlWriterConfig config) {
        super(config);
      }

      @Override
      public void dynamicParam(int index) {
        if (dynamicParameters == null) {
          dynamicParameters = new ArrayList<>();
        }
        dynamicParameters.add(index);
        print("$" + (index + 1));
        setNeedWhitespace(true);
      }
    }

    @Override
    public Object visitRoot(Root root, Object context) {
      root.getCoords().forEach(c->c.accept(this, context));
      return null;
    }

    @Override
    public Object visitTypeDefinition(TypeDefinitionSchema typeDefinitionSchema, Object context) {
      return null;
    }

    @Override
    public Object visitStringDefinition(StringSchema stringSchema, Object context) {
      return null;
    }

    @Override
    public Object visitArgumentLookup(ArgumentLookupCoords coords, Object context) {
      coords.getMatchs().forEach(c->{
        PgQuery query = c.getQuery().accept(this, context);
        c.setQuery(query);
      });
      return null;
    }

    @Override
    public Object visitFieldLookup(FieldLookupCoords coords, Object context) {
      return null;
    }

    @Override
    public PgQuery visitPgQuery(PgQuery pgQuery, Object context) {
      return null;
    }

    @Override
    public PgQuery visitPagedPgQuery(PagedPgQuery pgQuery, Object context) {
      return null;
    }

    @Override
    public Object visitSourcePgParameter(SourcePgParameter sourceParameter, Object context) {
      return null;
    }

    @Override
    public Object visitArgumentPgParameter(ArgumentPgParameter argumentParameter, Object context) {
      return null;
    }

    @Override
    public Object visitResolvedPgQuery(ResolvedPgQuery query, Object context) {
      return null;
    }

    @Override
    public Object visitResolvedPagedPgQuery(ResolvedPagedPgQuery query, Object context) {
      return null;
    }

    @Override
    public Object visitArgumentWrapper(GraphQLArgumentWrapper graphQLArgumentWrapper,
        Object context) {
      return null;
    }
  }
}
