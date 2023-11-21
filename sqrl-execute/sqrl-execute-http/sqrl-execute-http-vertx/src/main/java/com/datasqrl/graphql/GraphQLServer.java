/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.POST;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.kafka.KafkaSinkConsumer;
import com.datasqrl.graphql.kafka.KafkaSinkProducer;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.graphql.type.SqrlVertxScalars;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel model;
  private final int port;
  private final JdbcDataSystemConnector jdbc;
  private final NameCanonicalizer canonicalizer;

  public GraphQLServer() {
    this(readModel(), 8888, createClient(), NameCanonicalizer.SYSTEM);
  }

  public GraphQLServer(RootGraphqlModel model,
      int port, JdbcDataSystemConnector jdbc, NameCanonicalizer canonicalizer) {
    this.model = model;
    this.port = port;
    this.jdbc = jdbc;
    this.canonicalizer = canonicalizer;
  }

  @SneakyThrows
  private static RootGraphqlModel readModel() {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(
        new File("model.json"),
        RootGraphqlModel.class);
  }

  @SneakyThrows
  public static JdbcDataSystemConnector createClient() {
    ObjectMapper objectMapper = new ObjectMapper();
    JdbcDataSystemConnector jdbc = objectMapper.readValue(
        new File("config.json"),
        JdbcDataSystemConnector.class);
    return jdbc;
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

    SqlClient client = getSqlClient();

    GraphQL graphQL = createGraphQL(client, startPromise);

    router.route().handler(CorsHandler.create()
        .addOrigin("*")
        .allowedMethod(GET)
        .allowedMethod(POST));
    router.route().handler(BodyHandler.create());

    //Todo: Don't spin up the ws endpoint if there are no subscriptions
    GraphQLHandler graphQLHandler = GraphQLHandler.create(graphQL,
        graphQLHandlerOptions);
    router.route("/graphql").handler(graphQLHandler);

    HttpServerOptions httpServerOptions = new HttpServerOptions();

    if (!model.getSubscriptions().isEmpty()) {
      Handler apGraphQLHandler = ApolloWSHandler.create(graphQL);
      router.route("/graphql-ws").handler(apGraphQLHandler);
      httpServerOptions
          .addWebSocketSubProtocol("graphql-transport-ws")
          .addWebSocketSubProtocol("graphql-ws");
    }

    vertx.createHttpServer(httpServerOptions).requestHandler(router).listen(port)
        .onFailure((e)-> {
          log.error("Could not start graphql server", e);
          startPromise.fail(e);
        })
        .onSuccess((s)-> {
          log.info("HTTP server started on port {}", port);
          startPromise.complete();
        });
  }

  private SqlClient getSqlClient() {
    if (jdbc.getDialect().equalsIgnoreCase("postgres")) {
      return PgPool.client(vertx, toPgOptions(jdbc),
          new PgPoolOptions(new PoolOptions()));
    } else {
      return JDBCPool.pool(
          vertx,
          toJdbcConfig(jdbc),
          new PoolOptions());
    }
  }

  private JDBCConnectOptions toJdbcConfig(JdbcDataSystemConnector config) {
    JDBCConnectOptions options = new JDBCConnectOptions()
        .setJdbcUrl(jdbc.getUrl())
        .setDatabase(jdbc.getDatabase());

    Optional.ofNullable(config.getUser()).map(options::setUser);
    Optional.ofNullable(config.getPassword()).map(options::setPassword);
    return options;
  }

  private PgConnectOptions toPgOptions(JdbcDataSystemConnector jdbcConf) {
    PgConnectOptions options = new PgConnectOptions();
    options.setDatabase(jdbcConf.getDatabase());
    options.setHost(jdbcConf.getHost());
    options.setPort(jdbcConf.getPort());
    options.setUser(jdbcConf.getUser());
    options.setPassword(jdbcConf.getPassword());
    options.setCachePreparedStatements(true);
    options.setPipeliningLimit(100_000);
    return options;
  }

  public GraphQL createGraphQL(SqlClient client, Promise<Void> startPromise) {
    try {
      GraphQL graphQL = model.accept(
          new BuildGraphQLEngine(List.of(SqrlVertxScalars.JSON)),
          new VertxContext(new VertxJdbcClient(client), constructSinkProducers(model, vertx),
              constructSubscriptions(model, vertx), canonicalizer));
      return graphQL;
    } catch (Exception e) {
      startPromise.fail(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  static Map<String, SinkConsumer> constructSubscriptions(RootGraphqlModel root, Vertx vertx) {
    Map<String, SinkConsumer> consumers = new HashMap<>();
    for (SubscriptionCoords sub: root.getSubscriptions()) {
      consumers.put(sub.getFieldName(), KafkaSinkConsumer.createFromConfig(vertx,
          sub.getSinkConfig().deserialize(ErrorCollector.root())));
    }
    return consumers;
  }

  static Map<String, SinkProducer> constructSinkProducers(RootGraphqlModel root, Vertx vertx) {
    Map<String, SinkProducer> producers = new HashMap<>();
    for (MutationCoords mut : root.getMutations()) {
      producers.put(mut.getFieldName(), KafkaSinkProducer.createFromConfig(vertx,
          mut.getSinkConfig().deserialize(ErrorCollector.root())));
    }
    return producers;
  }
}
