/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.VertxGraphQLBuilder;
import com.datasqrl.graphql.server.VertxGraphQLBuilder.VertxContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel root;
  private final PgConnectOptions options;
  private final int port;
  private final PoolOptions poolOptions;

  public GraphQLServer(RootGraphqlModel root, PgConnectOptions options,
      int port, PoolOptions poolOptions) {
    this.root = root;
    this.options = options;
    this.port = port;
    this.poolOptions = poolOptions;
  }

  public GraphQLServer() {
    super();
    final JsonObject configs = vertx.getOrCreateContext().config();
    final JsonObject connectionConf = configs.getJsonObject("conn");
    final JsonObject poolConf = configs.getJsonObject("pool");
    final int port = configs.getInteger("port");

    ObjectMapper mapper = new ObjectMapper();

    this.root = mapper.convertValue(configs.getJsonObject("model"), RootGraphqlModel.class);
    this.options = new PgConnectOptions(connectionConf);
    this.poolOptions = new PoolOptions(poolConf);
    this.port = port;
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

    PgPool client = PgPool.pool(vertx, options, poolOptions);

    GraphQLHandler graphQLHandler = GraphQLHandler.create(createGraphQL(client),
        graphQLHandlerOptions);

    router.route("/graphql").handler(graphQLHandler);

    vertx.createHttpServer().requestHandler(router).listen(port, http -> {
      if (http.succeeded()) {
        log.info("HTTP server started on port {}", port);
      } else {
        startPromise.fail(http.cause());
      }
    });
  }

  @SneakyThrows
  private GraphQL createGraphQL(SqlClient client) {
    GraphQL graphQL = root.accept(
        new VertxGraphQLBuilder(),
        new VertxContext(client));
    return graphQL;
  }
}
