/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.graphql.kafka.KafkaSinkEmitter;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.Model;
import com.datasqrl.graphql.server.Model.KafkaMutationCoords;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.SinkEmitter;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandlerOptions;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerOptions;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel root;
  private final int port;
  private final JdbcDataSystemConnector jdbc;

  public GraphQLServer() {
    this(getModel(), 8888, getClient());
  }

  public GraphQLServer(RootGraphqlModel root,
      int port, JdbcDataSystemConnector jdbc) {
    this.root = root;
    this.port = port;
    this.jdbc = jdbc;
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

    SqlClient client = getSqlClient();
    JDBCPool.pool(this.vertx,
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

  @SneakyThrows
  public GraphQL createGraphQL(SqlClient client) {
    GraphQL graphQL = root.accept(
        new BuildGraphQLEngine(),
        new VertxContext(new VertxJdbcClient(client), constructSinkEmitters(root, vertx)));
    return graphQL;
  }

  private Map<String, SinkEmitter> constructSinkEmitters(RootGraphqlModel root, Vertx vertx) {

    Map<String, SinkEmitter> map = new HashMap<>();
    if (root.getMutations() != null) {
      for (MutationCoords m : root.getMutations()) {
        KafkaMutationCoords k = (KafkaMutationCoords) m;
        KafkaProducer<String, String> producer =
                KafkaProducer.create(vertx, k.getSinkConfig());
        map.put(k.getFieldName(), new KafkaSinkEmitter(producer,
                k.getSinkConfig().get("topic")));
      }
    }
    return map;
  }


}
