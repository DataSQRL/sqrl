/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.kafka.KafkaSinkConsumer;
import com.datasqrl.graphql.kafka.KafkaSinkProducer;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import com.datasqrl.graphql.type.SqrlVertxScalars;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import graphql.GraphQL;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    // Register the custom deserializer module
    SimpleModule module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    objectMapper.registerModule(module);
    return objectMapper.readValue(
        new File("server-model.json"),
        RootGraphqlModel.class);
  }

  public static class JsonEnvVarDeserializer extends JsonDeserializer<String> {

    @Override
    public String deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      String value = p.getText();
      Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
      Matcher matcher = pattern.matcher(value);
      StringBuffer result = new StringBuffer();
      while (matcher.find()) {
        String key = matcher.group(1);
        String envVarValue = System.getenv(key);
        if (envVarValue != null) {
          matcher.appendReplacement(result, envVarValue);
        }
      }
      matcher.appendTail(result);

      return result.toString();
    }
  }
  private Future<JsonObject> loadConfig() {
    Promise<JsonObject> promise = Promise.promise();
    vertx.fileSystem().readFile("server-config.json", result -> {
      if (result.succeeded()) {
        try {
          ObjectMapper objectMapper = new ObjectMapper();
          SimpleModule module = new SimpleModule();
          module.addDeserializer(String.class, new JsonEnvVarDeserializer());
          objectMapper.registerModule(module);
          Map configMap = objectMapper.readValue(result.result().toString(), Map.class);
          JsonObject config = new JsonObject(configMap);
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
      if (!startPromise.future().isComplete()) {
        startPromise.fail(e);
      }
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

    //Todo: Don't spin up the ws endpoint if there are no subscriptions
    SqlClient client = getSqlClient();
    GraphQL graphQL = createGraphQL(client, startPromise);

    CorsHandler corsHandler = toCorsHandler(this.config.getCorsHandlerOptions());
    router.route().handler(corsHandler);
    router.route().handler(BodyHandler.create());

    //Todo: Don't spin up the ws endpoint if there are no subscriptions
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
          if (!startPromise.future().isComplete()) {
            startPromise.fail(e);
          }
        })
        .onSuccess((s)-> {
          log.info("HTTP server started on port {}", this.config.getHttpServerOptions().getPort());
          if (!startPromise.future().isComplete()) {
            startPromise.complete();
          }
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
    return PgPool.client(vertx, this.config.getPgConnectOptions(),
        new PgPoolOptions(this.config.getPoolOptions())
            .setPipelined(true));
  }

  public GraphQL createGraphQL(SqlClient client, Promise<Void> startPromise) {
    try {
      GraphQL graphQL = model.accept(
          new GraphQLEngineBuilder(List.of(SqrlVertxScalars.JSON)),
          new VertxContext(new VertxJdbcClient(client), constructSinkProducers(model, vertx),
              constructSubscriptions(model, vertx, startPromise), canonicalizer));
      return graphQL;
    } catch (Exception e) {
      startPromise.fail(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  static Map<String, SinkConsumer> constructSubscriptions(RootGraphqlModel root, Vertx vertx,
      Promise<Void> startPromise) {
    Map<String, SinkConsumer> consumers = new HashMap<>();
    for (SubscriptionCoords sub: root.getSubscriptions()) {
      KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, sub.getSinkConfig());
      consumer.subscribe(sub.getTopic())
          .onSuccess(v -> log.info("Subscribed to topic: {}", sub.getTopic()))
          .onFailure(startPromise::fail);

      consumers.put(sub.getFieldName(), new KafkaSinkConsumer<>(consumer));
    }
    return consumers;
  }

  static Map<String, SinkProducer> constructSinkProducers(RootGraphqlModel root, Vertx vertx) {
    Map<String, SinkProducer> producers = new HashMap<>();
    for (MutationCoords mut : root.getMutations()) {
      KafkaProducer<String, String> producer = KafkaProducer.create(vertx, mut.getSinkConfig());
      KafkaSinkProducer sinkProducer = new KafkaSinkProducer<>(mut.getTopic(), producer);
      producers.put(mut.getFieldName(), sinkProducer);
    }
    return producers;
  }
}
