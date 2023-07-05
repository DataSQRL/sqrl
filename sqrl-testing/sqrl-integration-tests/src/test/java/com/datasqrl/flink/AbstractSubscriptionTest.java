package com.datasqrl.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.cmd.AssertStatusHook;
import com.datasqrl.cmd.RootCommand;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.engine.server.VertxEngineFactory.VertxEngine;
import com.datasqrl.engine.stream.flink.ExecutionEnvironmentFactory;
import com.datasqrl.engine.stream.flink.LocalFlinkStreamEngineImpl;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.kafka.KafkaLogEngine;
import com.datasqrl.kafka.KafkaLogEngineFactory;
import com.datasqrl.kafka.KafkaPhysicalPlan;
import com.datasqrl.kafka.NewTopic;
import com.datasqrl.packager.PackagerConfig;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.service.PackagerUtil;
import com.datasqrl.util.SnapshotTest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocket;
import io.vertx.core.impl.future.PromiseImpl;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.logging.log4j.util.Strings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@ExtendWith(VertxExtension.class)
public abstract class AbstractSubscriptionTest {

  public static final Consumer<HttpResponse<JsonObject>> FAIL_HANDLER = (h)->{
    if(h.statusCode() != 200 || h.body().containsKey("errors")) fail();
  };

  public static final Consumer<JsonObject> NO_WS_HANDLER = (h)->{};
  @Container
  protected final PostgreSQLContainer testDatabase = new PostgreSQLContainer(
      DockerImageName.parse("postgres:14.2")).withDatabaseName("foo").withUsername("foo")
      .withPassword("secret").withDatabaseName("datasqrl");

  public static final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(1);

  protected SnapshotTest.Snapshot snapshot;
  protected Vertx vertx;
  ObjectMapper mapper = new Deserializer().getJsonMapper();

  @BeforeEach
  public void setup(TestInfo testInfo, Vertx vertx) throws IOException {
    kafka.start();
    log.info("Kafka started: " + kafka.getAllTopicsInCluster());

    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
    this.vertx = vertx;
  }

  @SneakyThrows
  @AfterEach
  public void tearDown() {
    testDatabase.stop();
    kafka.stop();
    try {
      for (String id : vertx.deploymentIDs()) {
        vertx.undeploy(id).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
      }
      vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
    }
  }

  protected void executeRequests(String query, JsonObject input,
      Consumer<HttpResponse<JsonObject>> callback) {
    WebClient client = WebClient.create(vertx);
    JsonObject graphqlQuery = new JsonObject().put("query", query)
        .put("variables", input);

    client.post(8888, "localhost", "/graphql").putHeader("Content-Type", "application/json")
        .as(BodyCodec.jsonObject()).sendJsonObject(graphqlQuery, ar -> {
          if (ar.succeeded()) {
            log.info("Received response with status code" + ar.result().statusCode());
            log.info("Response Body: " + ar.result().body());
            if (ar.result().statusCode() != 200 || ar.result().body().toString().contains("errors")) {
              fail(ar.result().body().toString());
            }
            callback.accept(ar.result());
          } else {
            log.info("Something went wrong " + ar.cause().getMessage());
            fail();
          }
        });
  }

  protected PromiseImpl listenOnWebsocket(String query, Consumer<JsonObject> successHandler) {
    PromiseImpl p = new PromiseImpl();
    vertx.createHttpClient().webSocket(8888, "localhost", "/graphql-ws", websocketRes -> {
      log.info("Websocket connected: " + websocketRes.succeeded());
      if (websocketRes.succeeded()) {
        WebSocket websocket = websocketRes.result();

        JsonObject connectionInitPayload = new JsonObject();
        JsonObject payload = new JsonObject();
        payload.put("query", query);

        // Connection initialization
        connectionInitPayload.put("type", "connection_init");
        connectionInitPayload.put("payload", new JsonObject());
        websocket.writeTextMessage(connectionInitPayload.encode());

        // Start GraphQL subscription
        JsonObject startMessage = new JsonObject();
        startMessage.put("id", "1");
        startMessage.put("type", "start");
        startMessage.put("payload", payload);
        websocket.writeTextMessage(startMessage.encode());

        // Handle incoming messages
        websocket.handler(message -> {
          JsonObject jsonMessage = new JsonObject(message.toString());
          String messageType = jsonMessage.getString("type");
          switch (messageType) {
            case "connection_ack":
              log.info("Connection acknowledged");
              break;
            case "data":
              JsonObject result = jsonMessage.getJsonObject("payload").getJsonObject("data");
              log.info("Data: " + result);
              successHandler.accept(result);
              break;
            case "complete":
              log.info("Server indicated subscription complete");
              break;
            default:
              log.info("Received message: " + message);
          }
        });
        p.complete();
      } else {
        log.info("Failed to connect " + websocketRes.cause());
        fail();
        p.fail("Failed");
      }
    });
    return p;
  }

  protected void compile(Path rootDir) {
    Optional<Path> script = findFile(rootDir, ".sqrl");
    Optional<Path> graphql = findFile(rootDir, ".graphqls");

    if (script.isEmpty() && graphql.isEmpty()) {
      throw new RuntimeException("Could not find file: script<" + script + "> graphqls<" + graphql + ">");
    }
    compile(rootDir, script.get(), graphql.get());
  }

  protected void compile(Path rootDir, Path script, Path graphql) {
    Path override = createPackageOverride(kafka, testDatabase);
    Path defaultPackage = createDefaultPackage(rootDir, script, graphql);

    picocli.CommandLine rootCommand = new RootCommand(rootDir, AssertStatusHook.INSTANCE).getCmd();
    int code = rootCommand.execute("compile", script.toString(), graphql.toString(), "-c",
        defaultPackage.toAbsolutePath().toString(), "-c", override.toAbsolutePath().toString(), "--nolookup");
    assertEquals(0, code, "Non-zero exit code");
  }

  @SneakyThrows
  private Optional<Path> findFile(Path rootDir, String postfix) {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootDir)) {
      for (Path file : stream) {
        if (!Files.isDirectory(file) && file.getFileName().toString().endsWith(postfix)) {
          return Optional.of(file);
        }
      }
    }
    return Optional.empty();
  }

  //TODO: Migrate to pipeline deserializer
  @SneakyThrows
  protected CompletableFuture<ExecutionResult> executePipeline(Path rootDir) {
    SqrlConfig config = SqrlConfigCommons.fromFiles(ErrorCollector.root(),
        rootDir.resolve("build").resolve("package.json"));

    Path deployDir = rootDir.resolve("build").resolve("deploy");

    SqrlConfig eng = config.getSubConfig("engines");
    List<SqlDDLStatement> schema = Arrays.stream(
            Files.readString(deployDir.resolve("database-schema.sql")).split("\n"))
        .filter(s -> !Strings.isEmpty(s)).map(s -> (SqlDDLStatement) () -> s)
        .collect(Collectors.toList());
    JDBCPhysicalPlan jdbcPhysicalPlan = new JDBCPhysicalPlan(schema, Map.of());
    JDBCEngineFactory jdbcEngineFactory = new JDBCEngineFactory();
    JDBCEngine jdbcEngine = jdbcEngineFactory.initialize(eng.getSubConfig("database"));
    jdbcEngine.execute(jdbcPhysicalPlan, ErrorCollector.root()).get();

    List<NewTopic> kakfaPlan = mapper.readValue(deployDir.resolve("kafka-plan.json").toFile(),
        new TypeReference<List<NewTopic>>() {
        });
    KafkaLogEngineFactory logEngineFactory = new KafkaLogEngineFactory();
    KafkaLogEngine kafkaLogEngine = logEngineFactory.initialize(eng.getSubConfig("log"));
    kafkaLogEngine.execute(new KafkaPhysicalPlan(eng.getSubConfig("log"), kakfaPlan),
        ErrorCollector.root()).get();

    RootGraphqlModel model = mapper.readValue(deployDir.resolve("server-model.json").toFile(),
        RootGraphqlModel.class);
    JdbcDataSystemConnector jdbc = mapper.readValue(
        deployDir.resolve("server-config.json").toFile(), JdbcDataSystemConnector.class);
    ServerPhysicalPlan serverPhysicalPlan = new ServerPhysicalPlan(model, jdbc);
    VertxEngineFactory vertxEngineFactory = new VertxEngineFactory();
    VertxEngine vertxEngine = vertxEngineFactory.initialize(eng.getSubConfig("server"), vertx);
    vertxEngine.execute(serverPhysicalPlan, ErrorCollector.root()).get();

    FlinkExecutablePlan flinkPlan = mapper.readValue(deployDir.resolve("flink-plan.json").toFile(),
        FlinkExecutablePlan.class);
    FlinkStreamPhysicalPlan plan = new FlinkStreamPhysicalPlan(flinkPlan);
    LocalFlinkStreamEngineImpl localFlinkStreamEngine = new LocalFlinkStreamEngineImpl(
        new ExecutionEnvironmentFactory(Map.of()), config.getSubConfig("engines")
        .getSubConfig("stream"));
    CompletableFuture<com.datasqrl.engine.ExecutionResult> fut = localFlinkStreamEngine.execute(
        plan, ErrorCollector.root());
//
//    try {
//      fut.get(10, TimeUnit.SECONDS);
//    } catch (Exception e) {
//      //give flink some time to start
//    }

    return fut;
  }

  @SneakyThrows
  private Path createDefaultPackage(Path rootDir, Path script, Path graphql) {
    SqrlConfig config = PackagerUtil.createDockerConfig(null, null, ErrorCollector.root());
    PackagerConfig.PackagerConfigBuilder pkgBuilder = PackagerConfig.builder().rootDir(rootDir)
        .config(config).mainScript(script).graphQLSchemaFile(graphql);
    PackagerConfig packagerConfig = pkgBuilder.build();

    Path defaultPackage = Files.createTempFile("pkJson", ".json");
    config.toFile(defaultPackage, true);
    return defaultPackage;
  }

  private Path createPackageOverride(EmbeddedKafkaCluster kafka, PostgreSQLContainer testDatabase) {
    Map overrideConfig = Map.of("engines",
        Map.of("log", Map.of("connector",
            Map.of("bootstrap.servers", "localhost:" + kafka.bootstrapServers().split(":")[1])),
        "database", toDbMap(testDatabase)));
    return writeJson(overrideConfig);
  }

  private Map toDbMap(PostgreSQLContainer testDatabase) {
    return Map.of("database", testDatabase.getDatabaseName(),
        "dialect", "postgres",
        "driver", testDatabase.getDriverClassName(),
        "port", testDatabase.getMappedPort(5432),
        "host", testDatabase.getHost(),
        "user", testDatabase.getUsername(),
        "password", testDatabase.getPassword(),
        "url", testDatabase.getJdbcUrl());
  }

  @SneakyThrows
  private Path writeJson(Map overrideConfig) {
    Path tmp = Files.createTempFile("pkjson", ".json");
    mapper.writeValue(tmp.toFile(), overrideConfig);
    return tmp;
  }
}
