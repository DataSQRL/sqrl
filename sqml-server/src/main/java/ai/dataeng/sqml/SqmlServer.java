package ai.dataeng.sqml;

import ai.dataeng.sqml.GraphqlEndpoint.EndpointDefinition;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.ScriptAnalyzer;
import ai.dataeng.sqml.connector.HttpSource;
import ai.dataeng.sqml.connector.JDBCSink;
import ai.dataeng.sqml.connector.Sink;
import ai.dataeng.sqml.connector.Source;
import ai.dataeng.sqml.ingest.HttpIngest;
import ai.dataeng.sqml.materialization.GqlQuery;
import ai.dataeng.sqml.materialization.Plan;
import ai.dataeng.sqml.materialization.ValidationSchema;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.metadata.PostgresMetadataResolver;
import ai.dataeng.sqml.sql.parser.SqlParser;
import ai.dataeng.sqml.sql.tree.Script;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Builder;

@Builder
public class SqmlServer {

  private final List<Source> sources;
  private final Sink sink;
  private final Script script;
  private final Plan plan;
  private final ValidationSchema validationSchema;
  private final List<GqlQuery> queries;
  private final Session session;
  private final SqlParser sqlParser;
  private final Metadata metadata;

  ExecutorService executor =
      Executors.newFixedThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
      });
  ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(1);

  private String graphqlVerticle;
  private GraphqlEndpoint graphqlEndpoint;
  private ScheduledFuture<?> refreshFuture;

  public SqmlServer(List<Source> sources, Sink sink,
      Script script, Plan plan,
      ValidationSchema validationSchema, List<GqlQuery> queries,
      SqlParser sqlParser) {
    this.sources = sources;
    this.sink = sink;
    this.script = script;
    this.plan = plan;
    this.validationSchema = validationSchema;
    this.queries = queries;
    this.session = sink.createSession();
    this.sqlParser = sqlParser;
    this.metadata = new MetadataManager(session, new PostgresMetadataResolver(session));
  }

  public void run() {
    System.setProperty("vertxweb.environment", "dev");
    System.out.println("Running sqml server");

    Analysis analysis = null;//analyze(script);

    SchemaManager schemaManager = new SchemaManager(metadata);
    schemaManager.init(sources, script, analysis);

    executor.execute(new HttpIngest((HttpSource) sources.get(0), (JDBCSink) sink, session, metadata));

    List<EndpointDefinition> endpointDefinitions = new ArrayList<>();
    GraphQL graphQL = buildGraphql(analysis);
    endpointDefinitions.add(new EndpointDefinition(graphQL, script));

    this.graphqlEndpoint = new GraphqlEndpoint(endpointDefinitions);
    Vertx.vertx().deployVerticle(graphqlEndpoint)
        .onComplete(this::setGraphqlVerticle);

    //Todo
    // Analysis Model -> View translation
    // View translation -> Graphql Query
    // Script + Metadata -> Analysis Model
  }

//  private ScriptAnalysis analyze(Script script) {
//    ScriptAnalyzer scriptAnalyzer = new ScriptAnalyzer(session, sqlParser, null, metadata);
//    ScriptAnalysis analysis = scriptAnalyzer.analyze(script);
//    return analysis;
//  }

  public void stop() {
    refreshFuture.cancel(false);
  }

  private GraphQL buildGraphql(Analysis analysis) {
    SqlGraphqlSchema graphqlSchema = new SqlGraphqlSchema(analysis, metadata);
    //todo: add coderegistry
//    GraphQLSchema s = schema.codeRegistry(newCodeRegistry()
//        .dataFetcher(
//            coordinates("Query", "orders"),
//            fooDataFetcher
//        )
//        .build()).build();
    GraphQLSchema graphQLSchema = graphqlSchema.build().build();
    return GraphQL.newGraphQL(graphQLSchema).build();
  }

  private void setGraphqlVerticle(AsyncResult<String> id) {
    if (id.succeeded()) {
      this.graphqlVerticle = id.result();
      startRefresh();
    }
  }

  private void startRefresh() {
    //Todo: refresh schema only when metadata changes have occurred
    Runnable refreshSchema = () -> {
      try {
        Analysis analysis = null;//analyze(script);
        GraphQL graphQL = buildGraphql(analysis);
        graphqlEndpoint.refreshSchema(new EndpointDefinition(graphQL, script));
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
    this.refreshFuture = scheduler.scheduleAtFixedRate(refreshSchema, 0, 5, TimeUnit.SECONDS);
  }
}
