/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.kafka.KafkaSinkConsumer;
import com.datasqrl.graphql.kafka.KafkaSinkProducer;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.PreparsedQuery;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.graphql.type.SqrlVertxScalars;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.ExecutionInput;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel model;
  private final NameCanonicalizer canonicalizer;
  private ServerConfig config;

  public GraphQLServer() {
    this(readModel(), null, NameCanonicalizer.SYSTEM);
  }

  public GraphQLServer(RootGraphqlModel model, ServerConfig config, NameCanonicalizer canonicalizer) {
    this.model = model;
    this.config = config;
    this.canonicalizer = canonicalizer;
  }

  @SneakyThrows
  private static RootGraphqlModel readModel() {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(
        new File("server-model.json"),
        RootGraphqlModel.class);
  }

  private Future<JsonObject> loadConfig() {
    Promise<JsonObject> promise = Promise.promise();
    vertx.fileSystem().readFile("server-config.json", result -> {
      if (result.succeeded()) {
        try {
          JsonObject config = new JsonObject(result.result().toString());
          promise.complete(config);
        } catch (Exception e) {
          e.printStackTrace();
          promise.fail(e);
        }
      } else {
        promise.fail(result.cause());
      }
    });
    return promise.future();
  }

  @Override
  public void start(Promise<Void> startPromise) {
    if (this.config == null) {
      // Config not provided, load from file
      loadConfig().onComplete(ar -> {
        if (ar.succeeded()) {
          this.config = new ServerConfig(ar.result());
          trySetupServer(startPromise);
        } else {
          startPromise.fail(ar.cause());
        }
      });
    } else {
      // Config already provided, proceed with setup
      trySetupServer(startPromise);
    }
  }

  protected void trySetupServer(Promise<Void> startPromise) {
    try {
      setupServer(startPromise);
    } catch (Exception e) {
      log.error("Could not start graphql server", e);
      startPromise.fail(e);
    }
  }

  protected void setupServer(Promise<Void> startPromise) {
    Router router = Router.router(vertx);
    router.route().handler(LoggerHandler.create());
    if (this.config.getGraphiQLHandlerOptions() != null) {
      router.route(this.config.getServletConfig().getGraphiQLEndpoint())
          .handler(GraphiQLHandler.create(this.config.getGraphiQLHandlerOptions()));
    }

    router.errorHandler(500, ctx -> {
      ctx.failure().printStackTrace();
      ctx.response().setStatusCode(500).end();
    });

    SqlClient client = getSqlClient();
    GraphQL graphQL = createGraphQL(client, startPromise);

    CorsHandler corsHandler = toCorsHandler(this.config.getCorsHandlerOptions());
    router.route().handler(corsHandler);
    router.route().handler(BodyHandler.create());

    if (config.getServletConfig().isAllowRest()) {
      for (PreparsedQuery preparsedQuery : model.getPreparsedQueries()) {
        router.route(String.format("/api/%s", preparsedQuery.getOperationName()))
            .handler(c -> handleGenericGraphQLRequest(c, preparsedQuery, graphQL));
      }
    }

    GraphQLHandler graphQLHandler = GraphQLHandler.create(graphQL,
        this.config.getGraphQLHandlerOptions());
    router.route(this.config.getServletConfig().getGraphQLEndpoint()).handler(graphQLHandler);

    if (!model.getSubscriptions().isEmpty()) {
      if (this.config.getServletConfig().isUseApolloWs()) {
        ApolloWSHandler apolloWSHandler = ApolloWSHandler.create(graphQL,
            this.config.getApolloWSOptions());
        router.route(this.config.getServletConfig().getGraphQLWsEndpoint())
            .handler(apolloWSHandler);
      } else {
        throw new RuntimeException("Only Apollo style websockets supported at this time");
      }
    }

    vertx.createHttpServer(this.config.getHttpServerOptions()).requestHandler(router)
        .listen(this.config.getHttpServerOptions().getPort())
        .onFailure((e)-> {
          log.error("Could not start graphql server", e);
          startPromise.fail(e);
        })
        .onSuccess((s)-> {
          log.info("HTTP server started on port {}", this.config.getHttpServerOptions().getPort());
          startPromise.complete();
        });
  }

  private void handleGenericGraphQLRequest(RoutingContext routingContext,
      PreparsedQuery preparsedQuery, GraphQL graphQL) {

    // Construct variables from parameters
    JsonObject variables = new JsonObject();
    routingContext.queryParams().forEach(entry -> {
      if (preparsedQuery.getParameters().stream().anyMatch(p -> p.getName().equals(entry.getKey()))) {
        variables.put(entry.getKey(), entry.getValue());
      }
    });

    ExecutionInput executionInput = ExecutionInput.newExecutionInput()
        .query(preparsedQuery.getQuery())
        .variables(variables.getMap())
        .build();

    graphQL.executeAsync(executionInput).thenAccept(result -> {
      if (result.getErrors().isEmpty()) {
        routingContext.response()
            .putHeader("Content-Type", "application/json")
            .end(Json.encodePrettily(result.getData()));
      } else {
        routingContext.response()
            .setStatusCode(500)
            .putHeader("Content-Type", "application/json")
            .end(Json.encodePrettily(result.getErrors()));
      }
    }).exceptionally(ex -> {
      routingContext.fail(ex);
      return null;
    });
  }

  private CorsHandler toCorsHandler(CorsHandlerOptions corsHandlerOptions) {
    CorsHandler corsHandler = corsHandlerOptions.getAllowedOrigin() != null
        ? CorsHandler.create(corsHandlerOptions.getAllowedOrigin())
        : CorsHandler.create();

    // Empty allowed origin list means nothing is allowed vs null which is permissive
    if (corsHandlerOptions.getAllowedOrigins() != null) {
      corsHandler
          .addOrigins(corsHandlerOptions.getAllowedOrigins());
    }

    return corsHandler
        .allowedMethods(corsHandlerOptions.getAllowedMethods()
            .stream()
            .map(HttpMethod::valueOf)
            .collect(Collectors.toSet()))
        .allowedHeaders(corsHandlerOptions.getAllowedHeaders())
        .exposedHeaders(corsHandlerOptions.getExposedHeaders())
        .allowCredentials(corsHandlerOptions.isAllowCredentials())
        .maxAgeSeconds(corsHandlerOptions.getMaxAgeSeconds())
        .allowPrivateNetwork(corsHandlerOptions.isAllowPrivateNetwork());
  }

  private SqlClient getSqlClient() {
    if (this.config.getPgConnectOptions() != null && this.config.getServletConfig().isUsePgPool()) {
      return PgPool.client(vertx, this.config.getPgConnectOptions(),
          new PgPoolOptions(this.config.getPoolOptions())
              .setPipelined(true));
    } else if (this.config.getJdbcConnectOptions() != null) {
      return JDBCPool.pool(vertx, this.config.getJdbcConnectOptions(),
          this.config.getPoolOptions());
    } else {
      throw new RuntimeException("Unknown connection type. Either pgConnectOptions or jdbcConnectOptions should be configured.");
    }
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
