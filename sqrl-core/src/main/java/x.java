//package ai.datasqrl.graphql2.server;
//
//import graphql.GraphQL;
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Future;
//import io.vertx.core.Launcher;
//import io.vertx.core.Promise;
//import io.vertx.ext.web.Router;
//import io.vertx.ext.web.handler.BodyHandler;
//import io.vertx.ext.web.handler.LoggerHandler;
//import io.vertx.ext.web.handler.graphql.GraphQLHandler;
//import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
//import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
//import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
//import io.vertx.pgclient.PgConnectOptions;
//import io.vertx.pgclient.PgConnection;
//import io.vertx.sqlclient.impl.SqlClientInternal;
//
//public class StandaloneGraphQLServer extends AbstractVerticle {
//
//  private SqlClientInternal client;
//
//  public static void main(String[] args) {
//    Launcher.executeCommand("run", StandaloneGraphQLServer.class.getName(), "--conf", "config.json");
//  }
//
//  @Override
//  public void start(Promise<Void> startPromise) {
//    config();
//    GraphQLHandlerOptions graphQLHandlerOptions = new GraphQLHandlerOptions().setRequestBatchingEnabled(
//        true);
//
//    Router router = Router.router(vertx);
//    router.route().handler(LoggerHandler.create());
//    router.post().handler(BodyHandler.create());
//
//    GraphiQLHandlerOptions graphiQLHandlerOptions = new GraphiQLHandlerOptions().setEnabled(true);
//    router.route("/graphiql/*").handler(GraphiQLHandler.create(graphiQLHandlerOptions));
//
//    router.errorHandler(500, ctx -> {
//      ctx.failure().printStackTrace();
//      ctx.response().setStatusCode(500).end();
//    });
//
//    /* Start server on port 8888 */
//    int port = config().getInteger("http.port", 8888);
//
//    PgConnectOptions options = new PgConnectOptions();
//    options.setDatabase(config().getString("database", "henneberger"));
//    options.setHost(config().getString("host", "localhost"));
//    options.setPort(config().getInteger("port", 5432));
//    options.setUser(config().getString("username", "henneberger"));
////    options.setPassword(config().getString("password", "benchmarkdbpass"));
//    options.setCachePreparedStatements(true);
//    options.setPipeliningLimit(100_000);
//    PgConnection.connect(vertx, options).flatMap(conn -> {
//      client = (SqlClientInternal) conn;
//
//      GraphQLHandler graphQLHandler = GraphQLHandler.create(createGraphQL(client),
//          graphQLHandlerOptions);
//
//      router.route("/graphql").handler(graphQLHandler);
//
//      vertx.createHttpServer().requestHandler(router).listen(port, http -> {
//        if (http.succeeded()) {
//          System.out.println(String.format("HTTP server started on port %s", port));
//        } else {
//          startPromise.fail(http.cause());
//        }
//      });
//      return Future.succeededFuture();
//    }).onComplete(ar -> startPromise.complete()).onFailure(f -> startPromise.fail(f));
//
//  }
//
//  private GraphQL createGraphQL(SqlClientInternal client) {
//    return null;
////    return BetterModel.root.accept(
////        new VertxGraphQLBuilder(),
////        new VertxContext(client));
//  }
//}
