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
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class DatasqrlRun {
  // Fix override
  Path build = Path.of(System.getProperty("user.dir")).resolve("build");
  Path path = build.resolve("plan");

  EmbeddedKafkaCluster CLUSTER;
  ObjectMapper objectMapper = new ObjectMapper();
  PostgreSQLContainer postgreSQLContainer;
  AtomicBoolean isStarted = new AtomicBoolean(false);
  public static void main(String[] args) {
    DatasqrlRun run = new DatasqrlRun();
    run.run(true);
  }

  public DatasqrlRun() {
  }

  @VisibleForTesting
  public void setPath(Path path) {
    this.path = path;
    this.build = path.getParent();
  }

  public void run(boolean hold) {
    startPostgres();
    startKafka();

    // Register the custom deserializer module
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(String.class,
        new JsonEnvVarDeserializer(getEnv()));
    objectMapper.registerModule(module);

    startVertx();
    CompiledPlan plan = startFlink();
    TableResult execute = plan.execute();
    if (hold) {
      execute.print();
    }
  }

  @SneakyThrows
  public CompiledPlan startFlink() {
    CompiledPlan compileFlink = compileFlink();
    return compileFlink;
  }

  @SneakyThrows
  public CompiledPlan compileFlink() {
    //Read conf if present
    Path packageJson = build.resolve("package.json");
    Map<String, String> config = new HashMap<>();
    if (packageJson.toFile().exists()) {
      Map packageJsonMap = getPackageJson();
      Object o = packageJsonMap.get("values");
      if (o instanceof Map) {
        Object c = ((Map)o).get("flink-config");
        if (c instanceof Map) {
          config.putAll((Map)c);
        }
      }
    }

    config.putIfAbsent("table.exec.source.idle-timeout", "1 s");
    config.putIfAbsent("taskmanager.network.memory.max", "800m");
    config.putIfAbsent("execution.checkpointing.interval", "30 sec");
    config.putIfAbsent("state.backend", "rocksdb");
    config.putIfAbsent("table.exec.resource.default-parallelism", "1");

    Configuration configuration = Configuration.fromMap(config);
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
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

  @SneakyThrows
  private Map getPackageJson() {
    return objectMapper.readValue(build.resolve("package.json").toFile(), Map.class);
  }

  Map<String, String> getEnv() {
    Map<String, String> configMap = new HashMap<>();

    configMap.putAll(System.getenv());
    configMap.put("PROPERTIES_BOOTSTRAP_SERVERS", CLUSTER.bootstrapServers());
    configMap.put("PROPERTIES_GROUP_ID", "mygroupid");
    configMap.put("JDBC_URL", isStarted.get() ? postgreSQLContainer.getJdbcUrl() : "jdbc:postgresql://127.0.0.1:5432/datasqrl");
    configMap.put("JDBC_USERNAME", "postgres");
    configMap.put("JDBC_PASSWORD", "postgres");
    //todo target?
    configMap.put("DATA_PATH", build.resolve("deploy/flink/data").toString());
    configMap.put("PGHOST", "localhost");
    configMap.put("PGUSER", "postgres");
    configMap.put("PGPORT", postgreSQLContainer.getMappedPort(5432).toString());
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

    if (path.resolve("kafka.json").toFile().exists()) {
      Map map = objectMapper.readValue(path.resolve("kafka.json").toFile(), Map.class);
      List<Map<String, Object>> topics = (List<Map<String, Object>>) map.get("topics");
      for (Map<String, Object> topic : topics) {
        CLUSTER.createTopic((String) topic.get("name"), 1, 1);
      }
    }
  }

  @SneakyThrows
  public void startPostgres() {
    if (path.resolve("postgres.json").toFile().exists()) { //todo fix

      postgreSQLContainer = (PostgreSQLContainer)new PostgreSQLContainer(
          DockerImageName.parse("ankane/pgvector:v0.5.0")
              .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("datasqrl")
          .withPassword("postgres")
          .withUsername("postgres")
          .withEnv("POSTGRES_INITDB_ARGS", "--data-checksums")
          .withEnv("POSTGRES_HOST_AUTH_METHOD", "trust")
          .withCommand("postgres -c wal_level=logical");

      Map map = objectMapper.readValue(path.resolve("postgres.json").toFile(), Map.class);
      List<Map<String, Object>> ddl = (List<Map<String, Object>>) map.get("ddl");
      Connection connection;
      try {
        postgreSQLContainer.start();
        connection = postgreSQLContainer.createConnection("");
        isStarted.set(true);
        connection.createStatement().execute("ALTER SYSTEM SET wal_level = logical;");
        connection.createStatement().execute("ALTER SYSTEM SET max_replication_slots = 4;");
        connection.createStatement().execute("ALTER SYSTEM SET max_wal_senders = 4;");
        connection.createStatement().execute("ALTER SYSTEM SET wal_sender_timeout = 0;");
        connection.createStatement().execute("SELECT pg_reload_conf();");

      } catch (Exception e) {
        //attempt local connection
        // todo: install postgres in homebrew (?), also remove the database on shutdown or reinit
        connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/datasqrl",
            "postgres", "postgres");

      }

      for (Map<String, Object> statement : ddl) {
        String sql = (String) statement.get("sql");
        connection.createStatement().execute(sql);
      }

      if (path.resolve("postgres_log.json").toFile().exists()) { //todo fix\
        Map logs = objectMapper.readValue(path.resolve("postgres_log.json").toFile(), Map.class);
        List<Map> log = (List<Map>)logs.get("ddl");
        for (Map m : log) {
          String sql = (String) m.get("sql");
          connection.createStatement().execute(sql);
        }
      }
    }
  }

  @SneakyThrows
  public void startVertx() {
    if (!path.resolve("vertx.json").toFile().exists()) {
      return;
    }
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
        NameCanonicalizer.SYSTEM, getSnowflakeUrl()) {
      @Override
      public String getEnvironmentVariable(String envVar) {
        if (envVar.equalsIgnoreCase("PROPERTIES_BOOTSTRAP_SERVERS")) {
          return CLUSTER.bootstrapServers();
        }
        return null;
      }
    };

    PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(
        PrometheusConfig.DEFAULT);
    MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
        .setMicrometerRegistry(prometheusMeterRegistry)
        .setEnabled(true);

    Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

    vertx.deployVerticle(server, res -> {
      if (res.succeeded()) {
        System.out.println("Deployment id is: " + res.result());
      } else {
        System.out.println("Deployment failed!");
      }
    });
  }

  public Optional<String> getSnowflakeUrl() {
    Map engines = (Map)getPackageJson().get("engines");
    Map snowflake = (Map)engines.get("snowflake");
    if (snowflake != null) {
      Object url = snowflake.get("url");
      if (url instanceof String) {
        return Optional.of((String)url);
      }
    }

    return Optional.empty();
  }

  public static class ModelContainer {
    public RootGraphqlModel model;
  }
}