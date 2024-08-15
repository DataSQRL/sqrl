/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;


import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.JsonEnvVarDeserializer;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.StatementSetOperation;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class DatasqrlRun {
  Path path = Path.of("build", "plan");
  EmbeddedKafkaCluster CLUSTER;
  ObjectMapper objectMapper = new ObjectMapper();
  PostgreSQLContainer postgreSQLContainer;
  AtomicBoolean isStarted = new AtomicBoolean(false);
  public static void main(String[] args) {
    DatasqrlRun run = new DatasqrlRun();
    run.run();
  }

  public DatasqrlRun() {
  }

  @VisibleForTesting
  public void setPath(Path path) {
    this.path = path;
  }

  public void run() {
    startPostgres();
    startKafka();

    // Register the custom deserializer module
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(String.class,
        new JsonEnvVarDeserializer(getEnv()));
    objectMapper.registerModule(module);

    startVertx();
    startFlink();
  }

  @SneakyThrows
  public void startFlink() {
    CompiledPlan compileFlink = compileFlink();
    compileFlink.execute().print();
  }

  @SneakyThrows
  public CompiledPlan compileFlink() {
    Map<String, String> config = Map.of(
        "taskmanager.network.memory.max", "1g",
        "table.exec.source.idle-timeout", "1 s");
    //read flink config from package.json values?

    Configuration configuration = Configuration.fromMap(config);
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    sEnv.setParallelism(1);
    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(configuration).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    TableResult tableResult = null;

    Map map = objectMapper.readValue(path.resolve("flink.json").toFile(), Map.class);
    List<String> statements = (List<String>) map.get("flinkSql");

    for (int i = 0; i < statements.size()-1; i++) {
      String statement = statements.get(i);
      if (statement.trim().isEmpty()) {
        continue;
      }
//      System.out.println(replaceWithEnv(statement));
      tableResult = tEnv.executeSql(replaceWithEnv(statement));
    }
    String insert = replaceWithEnv(statements.get(statements.size() - 1));
    TableEnvironmentImpl tEnv1 = (TableEnvironmentImpl) tEnv;
    StatementSetOperation parse = (StatementSetOperation)tEnv1.getParser().parse(insert).get(0);

    CompiledPlan plan = tEnv1.compilePlan(parse.getOperations());

    return plan;
  }

  Map<String, String> getEnv() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("PROPERTIES_BOOTSTRAP_SERVERS", CLUSTER.bootstrapServers());
    configMap.put("PROPERTIES_GROUP_ID", "mygroupid");
    configMap.put("JDBC_URL", isStarted.get() ? postgreSQLContainer.getJdbcUrl() : "jdbc:postgresql://127.0.0.1:5432/datasqrl");
    configMap.put("JDBC_USERNAME", "postgres");
    configMap.put("JDBC_PASSWORD", "postgres");
    //todo target?
    configMap.put("DATA_PATH", Path.of(System.getProperty("user.dir")).resolve("build/deploy/flink/data").toString());
    configMap.put("PGHOST", "localhost");
    configMap.put("PGUSER", "postgres");
    configMap.put("PGPASSWORD", "postgres");
    configMap.put("PGDATABASE", "datasqrl");

    return configMap;
  }
  public String replaceWithEnv(String command) {
    Map<String, String> envVariables = getEnv();
    Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");

    String substitutedStr = command;
    StringBuffer result = new StringBuffer();
    // First pass to replace environment variables
    Matcher matcher = pattern.matcher(substitutedStr);
    while (matcher.find()) {
      String key = matcher.group(1);
      String envValue = envVariables.getOrDefault(key, "");
      matcher.appendReplacement(result, Matcher.quoteReplacement(envValue));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  @SneakyThrows
  public void startKafkaCluster() {
    CLUSTER = new EmbeddedKafkaCluster(1);
    CLUSTER.start();
  }

  @SneakyThrows
  public void startKafka() {
    startKafkaCluster();

    Map map = objectMapper.readValue(path.resolve("kafka.json").toFile(), Map.class);
    List<Map<String, Object>> topics = (List<Map<String, Object>>)map.get("topics");
    for (Map<String, Object> topic : topics) {
      CLUSTER.createTopic((String)topic.get("name"), 1, 1);
    }
  }

  @SneakyThrows
  public void startPostgres() {
    postgreSQLContainer = new PostgreSQLContainer(
      DockerImageName.parse("ankane/pgvector:v0.5.0")
        .asCompatibleSubstituteFor("postgres"))
        .withDatabaseName("datasqrl")
        .withPassword("postgres")
        .withUsername("postgres");

    Connection connection;
    try {
      postgreSQLContainer.start();
      connection = postgreSQLContainer.createConnection("");
      isStarted.set(true);
    } catch (Exception e) {
      //attempt local connection
      // todo: install postgres in homebrew (?), also remove the database on shutdown or reinit
      connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/datasqrl", "postgres", "postgres");
    }

    System.out.println(path.toAbsolutePath().toString());

    Map map = objectMapper.readValue(path.resolve("postgres.json").toFile(), Map.class);
    List<Map<String, Object>> ddl = (List<Map<String, Object>>) map.get("ddl");

    for (Map<String, Object> statement: ddl) {
      String sql = (String) statement.get("sql");
      connection.createStatement().execute(sql);
    }
  }


  @SneakyThrows
  public void startVertx() {
    RootGraphqlModel rootGraphqlModel = objectMapper.readValue(
        path.resolve("vertx.json").toFile(),
        ModelContainer.class).model;

    URL resource = Resources.getResource("server-config.json");
    Map json = objectMapper.readValue(
        resource,
        Map.class);
    JsonObject config = new JsonObject(json);

    ServerConfig serverConfig = new ServerConfig(config);
    // hack because templating doesn't work on non-strings
    serverConfig.getPgConnectOptions()
        .setPort(isStarted.get() ? postgreSQLContainer.getMappedPort(5432): 5432);
    GraphQLServer server = new GraphQLServer(rootGraphqlModel, serverConfig,
        NameCanonicalizer.SYSTEM) {
      @Override
      public String getEnvironmentVariable(String envVar) {
        if (envVar.equalsIgnoreCase("PROPERTIES_BOOTSTRAP_SERVERS")) {
          return CLUSTER.bootstrapServers();
        }
        return null;
      }
    };

    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(server);
  }

  public static class ModelContainer {
    public RootGraphqlModel model;
  }
}