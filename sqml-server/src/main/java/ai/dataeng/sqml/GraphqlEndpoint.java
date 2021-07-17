package ai.dataeng.sqml;


import ai.dataeng.sqml.sql.tree.Script;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import java.util.List;

public class GraphqlEndpoint extends AbstractVerticle {

  private final List<EndpointDefinition> endpointDefinitions;
  HttpServer server;
  private Router router;
  private Route graphQLRoute;

  public GraphqlEndpoint(List<EndpointDefinition> endpointDefinitions) {
    this.endpointDefinitions = endpointDefinitions;
  }

  @Override
  public void start(Promise<Void> promise) throws Exception {
    server = vertx.createHttpServer(createOptions());

    server.requestHandler(createRouter(endpointDefinitions));

    server.listen(res -> {
      if (res.failed()) {
        System.out.println("Started Graphql endpoint...Failed");
        promise.fail(res.cause());
      } else {
        System.out.println("Started Graphql endpoint");
        promise.complete();
      }
    });
  }

  private Router createRouter(List<EndpointDefinition> definitions) {
    Router router = Router.router(vertx);
    router.errorHandler(500, rc -> {
      System.err.println("failure handle  ");
      Throwable failure = rc.failure();
      if (failure != null) {
        failure.printStackTrace();
      }
    });
    this.router = router;
    router.route().handler(BodyHandler.create());
    this.graphQLRoute = router.post("/graphql").handler(GraphQLHandler.create(definitions.get(0).getGraphQL()));

    GraphiQLHandlerOptions options = new GraphiQLHandlerOptions()
        .setEnabled(true);

    router.route("/graphiql/*").handler(GraphiQLHandler.create(options));

    return router;
  }

  private HttpServerOptions createOptions() {
    HttpServerOptions serverOptions = new HttpServerOptions()
        .setPort(8080)
        .setHost("localhost");
    //todo: http2
//    serverOptions.setSsl(true)
//        .setKeyCertOptions(new PemKeyCertOptions().setCertPath("tls/server-cert.pem").setKeyPath("tls/server-key.pem"))
//        .setUseAlpn(true);
    return serverOptions;
  }

  public synchronized void refreshSchema(EndpointDefinition definition) {
    System.out.println("Refreshing Schema...");
    //todo: Handle multiple graphql instances?
    Route route = router.post("/graphql").handler(GraphQLHandler.create(definition.getGraphQL()));
    graphQLRoute.remove();
    graphQLRoute = route;
  }

  static class EndpointDefinition {

    private final GraphQL graphQL;
    private final Script script;

    public EndpointDefinition(GraphQL graphQL, Script script) {
      this.graphQL = graphQL;
      this.script = script;
    }

    public GraphQL getGraphQL() {
      return graphQL;
    }

    public Script getScript() {
      return script;
    }
  }
}
