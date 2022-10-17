package ai.datasqrl.graphql;

import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.server.VertxGraphQLBuilder;
import ai.datasqrl.graphql.server.VertxGraphQLBuilder.VertxContext;
import ai.datasqrl.util.db.JDBCTempDatabase;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Launcher;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.impl.SqlClientInternal;
import java.nio.file.Path;
import lombok.SneakyThrows;

public class GraphQLServer extends AbstractVerticle {

  private final Path build;
  private Root root;
  private JDBCTempDatabase tempDatabase;
  private SqlClientInternal client;

  public GraphQLServer(Path build, Root root, JDBCTempDatabase tempDatabase) {
    this.build = build;
    this.root = root;
    this.tempDatabase = tempDatabase;
  }

  public static void main(String[] args) {
    //todo: write out config in compile

    Launcher.executeCommand("run", GraphQLServer.class.getName() /*, "--conf", "config.json"*/);
  }

  @Override
  public void start(Promise<Void> startPromise) {
    GraphQLHandlerOptions graphQLHandlerOptions = new GraphQLHandlerOptions().setRequestBatchingEnabled(
        true);

    Router router = Router.router(vertx);
    router.route().handler(LoggerHandler.create());
    router.post().handler(BodyHandler.create());

    GraphiQLHandlerOptions graphiQLHandlerOptions = new GraphiQLHandlerOptions().setEnabled(true);
    router.route("/graphiql/*").handler(GraphiQLHandler.create(graphiQLHandlerOptions));

    router.errorHandler(500, ctx -> {
      ctx.failure().printStackTrace();
      ctx.response().setStatusCode(500).end();
    });

    /* Start server on port 8888 */
    int port = config().getInteger("http.port", 8888);

    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(config().getString("database", tempDatabase.getPostgreSQLContainer().getDatabaseName()));
    options.setHost(config().getString("host", tempDatabase.getPostgreSQLContainer().getHost()));
    options.setPort(config().getInteger("port",tempDatabase.getPostgreSQLContainer().getMappedPort(5432)));
    options.setUser(config().getString("username", tempDatabase.getPostgreSQLContainer().getUsername()));
    options.setPassword(config().getString("password", tempDatabase.getPostgreSQLContainer().getPassword()));
    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);
    PgConnection.connect(vertx, options).flatMap(conn -> {
      client = (SqlClientInternal) conn;

      GraphQLHandler graphQLHandler = GraphQLHandler.create(createGraphQL(client),
          graphQLHandlerOptions);

      router.route("/graphql").handler(graphQLHandler);

      vertx.createHttpServer().requestHandler(router).listen(port, http -> {
        if (http.succeeded()) {
          System.out.println(String.format("HTTP server started on port %s", port));
        } else {
          startPromise.fail(http.cause());
        }
      });
      return Future.succeededFuture();
    }).onSuccess(ar -> startPromise.complete())
        .onFailure(f -> {
          f.printStackTrace();
          startPromise.fail(f);
        });

  }

  @SneakyThrows
  private GraphQL createGraphQL(SqlClientInternal client) {
    ObjectMapper mapper = new ObjectMapper();

//    Root root = mapper.readValue(
//        build.resolve("api")
//            .resolve("plan.json").toFile(), Root.class);
//
    GraphQL graphQL = root.accept(
        new VertxGraphQLBuilder(),
        new VertxContext(client));
    return graphQL;
  }
}
