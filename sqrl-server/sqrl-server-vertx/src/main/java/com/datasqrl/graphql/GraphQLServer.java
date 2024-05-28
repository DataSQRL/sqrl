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
import com.datasqrl.graphql.calcite.FlinkQuery;
import com.datasqrl.graphql.calcite.FlinkSchema;
import com.datasqrl.graphql.calcite.SimpleModifiableTable;
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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import graphql.GraphQL;
import graphql.execution.instrumentation.ChainedInstrumentation;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import io.vertx.ext.web.handler.graphql.ApolloWSHandler;
import io.vertx.ext.web.handler.graphql.GraphQLHandler;
import io.vertx.ext.web.handler.graphql.GraphiQLHandler;
import io.vertx.ext.web.handler.graphql.instrumentation.JsonObjectAdapter;
import io.vertx.ext.web.handler.graphql.instrumentation.VertxFutureAdapter;
import io.vertx.ext.web.handler.graphql.ws.GraphQLWSHandler;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolOptions;
import io.vertx.sqlclient.SqlClient;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

@Slf4j
public class GraphQLServer extends AbstractVerticle {

  private final RootGraphqlModel model;
  private final NameCanonicalizer canonicalizer;
  private ServerConfig config;

  public GraphQLServer() {
    this(readModel(), null, NameCanonicalizer.SYSTEM);
  }

  public GraphQLServer(RootGraphqlModel model, ServerConfig config,
      NameCanonicalizer canonicalizer) {
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
    return objectMapper.readValue(new File("server-model.json"), RootGraphqlModel.class);
  }

  @SneakyThrows
  public static CalciteConnection getCalciteClient(Vertx vertx) {
    Class.forName("org.apache.calcite.jdbc.Driver");
//    JDBCPool pool = JDBCPool.pool(vertx, new JDBCConnectOptions().setJdbcUrl("jdbc:calcite:"),
//        new PoolOptions().setMaxSize(1));
//    SqlConnection sqlConnection = pool.getConnection().toCompletionStage().toCompletableFuture()
//        .get();
//    ;
//    CalciteConnection connection = (CalciteConnection)getInnermostConnection(sqlConnection);
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    FlinkSchema flinkSchema = new FlinkSchema("jdbc:flink://localhost:8083",0,0);
    rootSchema.add("flink", flinkSchema);
    JavaTypeFactoryImpl relDataTypeFactory = new JavaTypeFactoryImpl();
    flinkSchema.addTable(new FlinkQuery("sales_fact_1997",  List.of(
        "CREATE TABLE RandomData (\n" + " customerid INT, \n" + " text STRING \n" + ") WITH (\n"
            + " 'connector' = 'datagen',\n" +
            " 'number-of-rows'='10',\n"
            + " 'rows-per-second'='5',\n"
            + " 'fields.customerid.kind'='random',\n" + " 'fields.customerid.min'='1',\n"
            + " 'fields.customerid.max'='1000',\n" + " 'fields.text.length'='10'\n" + ")"),
        "SELECT * FROM RandomData", relDataTypeFactory.createStructType(StructKind.FULLY_QUALIFIED,
        List.of(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
            relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of("customerid", "text"))));

    rootSchema.add("mod", new SimpleModifiableTable(
        relDataTypeFactory.createStructType(StructKind.FULLY_QUALIFIED,
            List.of(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
                relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR)), List.of("customerid", "text"))
    ));
//
//    PreparedStatement preparedStatement = calciteConnection.prepareStatement("INSERT INTO \"mod\" "
//        + "VALUES(?, ?)"
////        + "SELECT customerid + '1', text FROM (VALUES(?, ?)) as t(customerid, text)"
//    );
//    preparedStatement.setString(1, "1");
//    preparedStatement.setString(2, "2");
//
//    System.out.println(preparedStatement.executeUpdate());
//
//    ResultSet resultSet = calciteConnection.createStatement()
//        .executeQuery("SELECT * FROM \"mod\" ");
//
//    resultSet.next();
//    System.out.println(resultSet.getString(1));

    flinkSchema.assureOpen();

    return calciteConnection;
  }
  public static Object getInnermostConnection(Object outerConnection) throws NoSuchFieldException, IllegalAccessException {
    Field field = null;
    Object currentObject = outerConnection;

    // Assuming you know the field names and their depths
    String[] fields = {"conn", "conn", "conn", "wrappedConnection"};
    for (String fieldName : fields) {
      field = currentObject.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      currentObject = field.get(currentObject);
    }

    return currentObject;
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

    GraphQL graphQL = createGraphQL(client, getCalciteClient(vertx), startPromise);

    CorsHandler corsHandler = toCorsHandler(this.config.getCorsHandlerOptions());
    router.route().handler(corsHandler);
    router.route().handler(BodyHandler.create());

    HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx);
    router.get("/health*").handler(healthCheckHandler);

    //Todo: Don't spin up the ws endpoint if there are no subscriptions
    GraphQLHandler graphQLHandler = GraphQLHandler.create(graphQL,
        this.config.getGraphQLHandlerOptions());

    if (!model.getSubscriptions().isEmpty()) {
      if (this.config.getServletConfig().isUseApolloWs()) {
        ApolloWSHandler apolloWSHandler = ApolloWSHandler.create(graphQL,
            this.config.getApolloWSOptions());
        if (this.config.getServletConfig().getGraphQLWsEndpoint()
            .equalsIgnoreCase(this.config.getServletConfig().getGraphQLEndpoint())) {
          router.route(this.config.getServletConfig().getGraphQLEndpoint()).handler(apolloWSHandler)
              .handler(graphQLHandler);
        } else {
          router.route(this.config.getServletConfig().getGraphQLWsEndpoint())
              .handler(apolloWSHandler);
          router.route(this.config.getServletConfig().getGraphQLEndpoint()).handler(graphQLHandler);
        }
      } else {
        GraphQLWSHandler graphQLWSHandler = GraphQLWSHandler.create(graphQL);
        router.route(this.config.getServletConfig().getGraphQLEndpoint()).handler(graphQLWSHandler)
            .handler(graphQLHandler);
      }
    } else {
      router.route(this.config.getServletConfig().getGraphQLEndpoint()).handler(graphQLHandler);
    }

    vertx.createHttpServer(this.config.getHttpServerOptions()).requestHandler(router)
        .listen(this.config.getHttpServerOptions().getPort()).onFailure((e) -> {
          log.error("Could not start graphql server", e);
          if (!startPromise.future().isComplete()) {
            startPromise.fail(e);
          }
        }).onSuccess((s) -> {
          log.info("HTTP server started on port {}", this.config.getHttpServerOptions().getPort());
          if (!startPromise.future().isComplete()) {
            startPromise.complete();
          }
        });
  }

  private CorsHandler toCorsHandler(CorsHandlerOptions corsHandlerOptions) {
    CorsHandler corsHandler = corsHandlerOptions.getAllowedOrigin() != null ? CorsHandler.create(
        corsHandlerOptions.getAllowedOrigin()) : CorsHandler.create();

    // Empty allowed origin list means nothing is allowed vs null which is permissive
    if (corsHandlerOptions.getAllowedOrigins() != null) {
      corsHandler.addOrigins(corsHandlerOptions.getAllowedOrigins());
    }

    return corsHandler.allowedMethods(
            corsHandlerOptions.getAllowedMethods().stream().map(HttpMethod::valueOf)
                .collect(Collectors.toSet())).allowedHeaders(corsHandlerOptions.getAllowedHeaders())
        .exposedHeaders(corsHandlerOptions.getExposedHeaders())
        .allowCredentials(corsHandlerOptions.isAllowCredentials())
        .maxAgeSeconds(corsHandlerOptions.getMaxAgeSeconds())
        .allowPrivateNetwork(corsHandlerOptions.isAllowPrivateNetwork());
  }

  private SqlClient getSqlClient() {
    return PgPool.client(vertx, this.config.getPgConnectOptions(),
        new PgPoolOptions(this.config.getPoolOptions()).setPipelined(true));
  }

  public GraphQL createGraphQL(SqlClient client, CalciteConnection calciteClient,
      Promise<Void> startPromise) {
    try {
      GraphQL graphQL = model.accept(new GraphQLEngineBuilder(List.of(SqrlVertxScalars.JSON)),
          new VertxContext(vertx, new VertxJdbcClient(client), calciteClient,
              constructSinkProducers(model, vertx),
              constructSubscriptions(model, vertx, startPromise), canonicalizer)).instrumentation(
          new ChainedInstrumentation(new JsonObjectAdapter(), VertxFutureAdapter.create())).build();
      return graphQL;
    } catch (Exception e) {
      startPromise.fail(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  Map<String, SinkConsumer> constructSubscriptions(RootGraphqlModel root, Vertx vertx,
      Promise<Void> startPromise) {
    Map<String, SinkConsumer> consumers = new HashMap<>();
    for (SubscriptionCoords sub : root.getSubscriptions()) {
      KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, getSourceConfig());
      consumer.subscribe(sub.getTopic())
          .onSuccess(v -> log.info("Subscribed to topic: {}", sub.getTopic()))
          .onFailure(startPromise::fail);

      consumers.put(sub.getFieldName(), new KafkaSinkConsumer<>(consumer));
    }
    return consumers;
  }

  private Map<String, String> getSourceConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(BOOTSTRAP_SERVERS_CONFIG, getEnvironmentVariable("PROPERTIES_BOOTSTRAP_SERVERS"));
    conf.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
    conf.put(KEY_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
    conf.put(VALUE_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
    conf.put(AUTO_OFFSET_RESET_CONFIG, "latest");
    return conf;
  }

  String getEnvironmentVariable(String envVar) {
    return System.getenv(envVar);
  }

  private Map<String, String> getSinkConfig() {
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
      KafkaProducer<String, String> producer = KafkaProducer.create(vertx, getSinkConfig());
      KafkaSinkProducer sinkProducer = new KafkaSinkProducer<>(mut.getTopic(), producer);
      producers.put(mut.getFieldName(), sinkProducer);
    }
    return producers;
  }

  public static class JsonEnvVarDeserializer extends JsonDeserializer<String> {

    @Override
    public String deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
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
}
