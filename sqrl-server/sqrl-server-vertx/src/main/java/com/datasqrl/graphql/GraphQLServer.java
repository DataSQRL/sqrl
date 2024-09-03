/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.kafka.KafkaSinkConsumer;
import com.datasqrl.graphql.kafka.KafkaSinkProducer;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import com.datasqrl.graphql.type.SqrlVertxScalars;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Strings;
import com.symbaloo.graphqlmicrometer.MicrometerInstrumentation;
import graphql.GraphQL;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandlerBuilder;
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSHandler;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.jdbcclient.impl.ConnectionImpl;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import io.vertx.micrometer.backends.BackendRegistry;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.SqlConnectionBase;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBDriver;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel model;
  private final NameCanonicalizer canonicalizer;
  private final Optional<String> snowflakeUrl;
  private ServerConfig config;

  public static void main(String[] args) {
    PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(
        PrometheusConfig.DEFAULT);
    MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
        .setMicrometerRegistry(prometheusMeterRegistry)
        .setEnabled(true);
    Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

    vertx.deployVerticle(new GraphQLServer(), res -> {
      if (res.succeeded()) {
        System.out.println("Deployment id is: " + res.result());
      } else {
        System.out.println("Deployment failed!");
      }
    });
  }

  public GraphQLServer() {
    this(readModel(), null, NameCanonicalizer.SYSTEM, readSnowflakeUrl());
  }

  @SneakyThrows
  private static Optional<String> readSnowflakeUrl() {
    //How to check for snowflake ?
    File snowflakeConfig = new File("snowflake-config.json");
    Map map = null;
    if (snowflakeConfig.exists()) {
      map = getObjectMapper().readValue(snowflakeConfig, Map.class);
      if (map.isEmpty()) return Optional.empty();
    } else {
      return Optional.empty();
    }

    String url = (String)map.get("url");
    if (Strings.isNullOrEmpty(url)) {
      log.warn("Url must be specified in the snowflake engine");
      return Optional.empty();
    }
    return Optional.of(url);
  }

  public GraphQLServer(RootGraphqlModel model, ServerConfig config, NameCanonicalizer canonicalizer,
      Optional<String> snowflakeUrl) {
    this.model = model;
    this.config = config;
    this.canonicalizer = canonicalizer;
    this.snowflakeUrl = snowflakeUrl;
  }

  @SneakyThrows
  private static RootGraphqlModel readModel() {
    return getObjectMapper().readValue(
        new File("server-model.json"),
        RootGraphqlModel.class);
  }

  public static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();

    // Register the custom deserializer module
    SimpleModule module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    objectMapper.registerModule(module);
    return objectMapper;
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
      e.printStackTrace();
      if (!startPromise.future().isComplete()) {
        startPromise.fail(e);
      }
    }
  }

  protected void setupServer(Promise<Void> startPromise) {
    Router router = Router.router(vertx);
    router.route().handler(LoggerHandler.create());

    MeterRegistry registry = BackendRegistries.getDefaultNow();
    if (registry instanceof PrometheusMeterRegistry) {
      router.route("/metrics").handler(ctx -> {
        ctx.response().putHeader("content-type", "text/plain");
        ctx.response().end(((PrometheusMeterRegistry)registry).scrape());
      });
    }

    if (this.config.getGraphiQLHandlerOptions() != null) {
      GraphiQLHandlerBuilder handlerBuilder = GraphiQLHandler.builder(vertx)
          .with(this.config.getGraphiQLHandlerOptions());
      if (this.config.getAuthOptions() != null) {
        handlerBuilder.addingHeaders(rc -> {
          String token = rc.get("token");
          return MultiMap.caseInsensitiveMultiMap().add("Authorization", "Bearer " + token);
        });
      }

      GraphiQLHandler handler = handlerBuilder.build();
      router.route(this.config.getServletConfig().getGraphiQLEndpoint())
          .subRouter(handler.router());
    }

    router.errorHandler(500, ctx -> {
      ctx.failure().printStackTrace();
      ctx.response().setStatusCode(500).end();
    });

    SqlClient client = getPostgresSqlClient();
    Map<String, SqlClient> clients = new HashMap<>();
    clients.put("postgres", client);
    clients.put("duckdb", getDuckdbSqlClient());
    snowflakeUrl.map(s-> clients.put("snowflake", getSnowflakeClient(s)));

    GraphQL graphQL = createGraphQL(clients, startPromise);

    CorsHandler corsHandler = toCorsHandler(this.config.getCorsHandlerOptions());
    router.route().handler(corsHandler);
    router.route().handler(BodyHandler.create());

    HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx);
    router.get("/health*").handler(healthCheckHandler);

    Route handler = router.route(this.config.getServletConfig().getGraphQLEndpoint());
    Optional<JWTAuth> authProvider = this.config.getAuthOptions() != null ?
        Optional.of(JWTAuth.create(vertx, this.config.getAuthOptions())) : Optional.empty();
    authProvider.ifPresent((auth)-> {
      //Required for adding auth on ws handler
      System.setProperty("io.vertx.web.router.setup.lenient", "true");
      handler.handler(JWTAuthHandler.create(auth));
    });
    handler.handler(GraphQLWSHandler.create(graphQL))
        .handler(GraphQLHandler.create(graphQL,this.config.getGraphQLHandlerOptions()));

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

  @SneakyThrows
  private SqlClient getSnowflakeClient(String url) {
    try {
      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    final JsonObject config = new JsonObject()
        .put("driver_class", "net.snowflake.client.jdbc.SnowflakeDriver")
        .put("url", url)
        .put("CLIENT_SESSION_KEEP_ALIVE", "true");

    JDBCPool pool = JDBCPool.pool(vertx, config);
    return pool;
  }

  @SneakyThrows
  private SqlClient getDuckdbSqlClient() {
    String url = "jdbc:duckdb:"; // In-memory DuckDB instance or you can specify a file path for persistence

    try {
      Class.forName("org.duckdb.DuckDBDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    final JsonObject config = new JsonObject()
        .put("driver_class", "org.duckdb.DuckDBDriver")
        .put("datasourceName", "pool-name")
        .put("url", url)
//        .put("max_pool_size", 1)
        .put(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true))
    ;

    JDBCPool pool = JDBCPool.pool(vertx, config);

    return pool;
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

  private SqlClient getPostgresSqlClient() {
    return PgPool.client(vertx, this.config.getPgConnectOptions(),
        new PgPoolOptions(this.config.getPoolOptions())
            .setPipelined(true));
  }

  public GraphQL createGraphQL(Map<String, SqlClient> client, Promise<Void> startPromise) {
    try {
      GraphQL.Builder graphQL = model.accept(
          new GraphQLEngineBuilder(List.of(SqrlVertxScalars.JSON)),
          new VertxContext(new VertxJdbcClient(client), constructSinkProducers(model, vertx),
              constructSubscriptions(model, vertx, startPromise), canonicalizer));
      MeterRegistry meterRegistry = BackendRegistries.getDefaultNow();
      if (meterRegistry != null) {
        graphQL.instrumentation(new MicrometerInstrumentation(meterRegistry));
      }
      return graphQL.build();
    } catch (Exception e) {
      startPromise.fail(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  Map<String, SinkConsumer> constructSubscriptions(RootGraphqlModel root, Vertx vertx,
      Promise<Void> startPromise) {
    Map<String, SinkConsumer> consumers = new HashMap<>();
    for (SubscriptionCoords sub: root.getSubscriptions()) {
      if (sub instanceof KafkaSubscriptionCoords) {
        KafkaSubscriptionCoords kafkaSub = (KafkaSubscriptionCoords) sub;
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, getSourceConfig());
        consumer.subscribe(kafkaSub.getTopic())
            .onSuccess(v -> log.info("Subscribed to topic: {}", kafkaSub.getTopic()))
            .onFailure(startPromise::fail);

        consumers.put(sub.getFieldName(), new KafkaSinkConsumer<>(consumer));
      } else if (sub instanceof PostgresSubscriptionCoords) {
        PostgresSubscriptionCoords pgSub = (PostgresSubscriptionCoords) sub;


      } else {
        throw new IllegalArgumentException("Unknown subscription type: " + sub.getClass());
      }
    }
    return consumers;
  }

  Map<String, String> getSourceConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(BOOTSTRAP_SERVERS_CONFIG, getEnvironmentVariable("PROPERTIES_BOOTSTRAP_SERVERS"));
    conf.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
    conf.put(KEY_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
    conf.put(VALUE_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
    conf.put(AUTO_OFFSET_RESET_CONFIG, "latest");
    return conf;
  }

  public String getEnvironmentVariable(String envVar) {
    return System.getenv(envVar);
  }

  Map<String, String> getSinkConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(BOOTSTRAP_SERVERS_CONFIG, getEnvironmentVariable("PROPERTIES_BOOTSTRAP_SERVERS"));
    conf.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
    conf.put(KEY_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");
    conf.put(VALUE_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");

    return conf;
  }

  Map<String, SinkProducer> constructSinkProducers(RootGraphqlModel root, Vertx vertx) {
    Map<String, SinkProducer> producers = new HashMap<>();
    for (MutationCoords mut : root.getMutations()) {
      if (mut instanceof KafkaMutationCoords) {
        KafkaMutationCoords kafkaMut = (KafkaMutationCoords) mut;
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, getSinkConfig());
        KafkaSinkProducer sinkProducer = new KafkaSinkProducer<>(kafkaMut.getTopic(), producer);
        producers.put(mut.getFieldName(), sinkProducer);
      }
    }
    return producers;
  }
}
