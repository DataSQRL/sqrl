/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.io.File;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel root;
  private final int port;
  private final JdbcDataSystemConnector jdbc;

  public GraphQLServer() {
    this(getModel(), 8888, getClient());
//    Path outputDir = root.rootDir.resolve(DEFAULT_DEPLOY_DIR);
//    ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
//    RootGraphqlModel model = mapper.readValue(outputDir.resolve(DEFAULT_SERVER_MODEL).toFile(), RootGraphqlModel.class);


  }

  @SneakyThrows
  private static RootGraphqlModel getModel() {

    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(
        new File("model.json"),
        RootGraphqlModel.class);
  }

  @SneakyThrows
  public static JdbcDataSystemConnector getClient() {
    ObjectMapper objectMapper = new ObjectMapper();
    JdbcDataSystemConnector jdbc = objectMapper.readValue(
        new File("config.json"),
        JdbcDataSystemConnector.class);
    return jdbc;
  }

  public GraphQLServer(RootGraphqlModel root,
      int port, JdbcDataSystemConnector jdbc) {
    this.root = root;
    this.port = port;
    this.jdbc = jdbc;
  }

  @SneakyThrows
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

    SqlClient client = JDBCPool.pool(this.vertx,
        new JDBCConnectOptions()
            .setJdbcUrl(jdbc.getUrl())
            .setDatabase(jdbc.getDatabase())
            .setUser(jdbc.getUser())
            .setPassword(jdbc.getPassword()),
        new PoolOptions());

    GraphQLHandler graphQLHandler = GraphQLHandler.create(createGraphQL(client),
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
        new BuildGraphQLEngine(),
        new VertxContext(new VertxJdbcClient(client)));
    return graphQL;
  }
}
