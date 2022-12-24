/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.SqrlGraphQLServer;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.sqlclient.SqlClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel root;
  private final int port;
  private final SqlClient sqlClient;

  public GraphQLServer(RootGraphqlModel root,
      int port, SqlClient sqlClient) {
    this.root = root;
    this.port = port;
    this.sqlClient = sqlClient;
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


    GraphQLHandler graphQLHandler = GraphQLHandler.create(createGraphQL(sqlClient),
        graphQLHandlerOptions);

    router.route("/graphql").handler(graphQLHandler);

    vertx.createHttpServer().requestHandler(router).listen(port)
        .onFailure((e)-> {
          log.error("Could not start graphql server", e);
          startPromise.fail(e);
        })
        .onSuccess((s)-> {
          log.info("HTTP server started on port {}", port);
          startPromise.complete();
        });
  }

  @SneakyThrows
  public GraphQL createGraphQL(SqlClient client) {
    GraphQL graphQL = root.accept(
        new SqrlGraphQLServer(),
        new VertxContext(new VertxJdbcClient(client)));
    return graphQL;
  }
}
