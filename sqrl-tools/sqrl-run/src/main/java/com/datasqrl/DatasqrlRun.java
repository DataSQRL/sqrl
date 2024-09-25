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
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public class DatasqrlRun {

  private final Map<String, String> env;
  // Fix override
  Path build = Path.of(System.getProperty("user.dir")).resolve("build");
  Path path = build.resolve("plan");

  ObjectMapper objectMapper = new ObjectMapper();

  Vertx vertx;
  TableResult execute;

  public static void main(String[] args) {
    DatasqrlRun run = new DatasqrlRun();
    run.run(true);
  }

  public DatasqrlRun() {
    this.env = System.getenv();
    //todo: Data_dir, kafka brokers
  }

  public DatasqrlRun(Path path, Map<String, String> env) {
    Map<String, String> newEnv = new HashMap<>();
    newEnv.putAll(System.getenv());
    newEnv.putAll(env);
    this.env = newEnv;
    setPath(path);
  }

  @VisibleForTesting
  public void setPath(Path path) {
    this.path = path;
    this.build = path.getParent();
  }

  public TableResult run(boolean hold) {
    initPostgres();
    initKafka();

    // Register the custom deserializer module
    objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(String.class,
        new JsonEnvVarDeserializer(env));
    objectMapper.registerModule(module);

    startVertx();
    CompiledPlan plan = startFlink();
    execute = plan.execute();
    if (hold) {
      execute.print();
    }
    return execute;
  }

  public void stop() {
    if (execute != null) {
      execute.getJobClient().get().cancel();
    }
    if (vertx != null) {
      vertx.close();
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
    config.putIfAbsent("execution.checkpointing.min-pause", "20 s");
    config.putIfAbsent("state.backend", "rocksdb");
    config.putIfAbsent("table.exec.resource.default-parallelism", "1");
    if (env.get("EXECUTION_MODE") != null) {
      config.putIfAbsent("execution.target", env.get("EXECUTION_MODE"));
    } else {
      config.putIfAbsent("execution.target", "remote");
    }
    config.putIfAbsent("rest.address", "localhost");

    Configuration configuration = Configuration.fromMap(config);

    // Read environment variables and collect JAR URLs
    String udfJarDirEnv = getenv("UDF_JAR_DIR");
    String systemJarDirEnv = getenv("SYSTEM_JAR_DIR");

    List<URL> jarURLs = new ArrayList<>();

    // Collect JARs from directories
    collectJarURLs(jarURLs, udfJarDirEnv);
    collectJarURLs(jarURLs, systemJarDirEnv);

    // Create URLClassLoader with collected JARs
    URLClassLoader urlClassLoader = new URLClassLoader(jarURLs.toArray(new URL[0]));

    //todo: check to see if execution.target is local or remote. If local, use getExe
    String mode = config.get("execution.target");
    StreamExecutionEnvironment sEnv;
    switch (mode){
      case "local":
        //todo udfs?
        sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        break;
      default:
      case "remote":
        sEnv = new StreamExecutionEnvironment(configuration, urlClassLoader);
    }

    sEnv.configure(configuration, urlClassLoader);

    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(configuration)
        .withClassLoader(urlClassLoader)
        .build();

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    TableResult tableResult = null;

    Map map = objectMapper.readValue(path.resolve("flink.json").toFile(), Map.class);
    List<String> statements = (List<String>) map.get("flinkSql");

    // Add JARs to the TableEnvironment
    addJarsToTableEnvironment(tEnv, jarURLs);

    for (int i = 0; i < statements.size()-1; i++) {
      String statement = statements.get(i);
      if (statement.trim().isEmpty()) {
        continue;
      }
      tableResult = tEnv.executeSql(replaceWithEnv(statement));
    }
    String insert = replaceWithEnv(statements.get(statements.size() - 1));

    TableEnvironmentImpl tEnv1 = (TableEnvironmentImpl) tEnv;

    StatementSetOperation parse = (StatementSetOperation)tEnv1.getParser().parse(insert).get(0);

    return tEnv1.compilePlan(parse.getOperations());
  }

  private void collectJarURLs(List<URL> jarURLs, String dirEnvVar) {
    if (dirEnvVar != null && !dirEnvVar.isEmpty()) {
      Path dirPath = Paths.get(dirEnvVar);
      if (Files.exists(dirPath) && Files.isDirectory(dirPath)) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, "*.jar")) {
          for (Path jarFile : stream) {
            URL jarURL = jarFile.toUri().toURL();
            jarURLs.add(jarURL);
          }
        } catch (IOException e) {
          // Handle exception or log it
          e.printStackTrace();
        }
      }
    }
  }

  @SneakyThrows
  private void addJarsToTableEnvironment(StreamTableEnvironment tEnv, List<URL> jarURLs) {
    for (URL jarURL : jarURLs) {
      String jarPath = jarURL.toURI().getPath();
      String addJarStatement = "ADD JAR '" + jarPath + "'";
      tEnv.executeSql(addJarStatement);
    }
  }
  @SneakyThrows
  protected Map getPackageJson() {
    return objectMapper.readValue(build.resolve("package.json").toFile(), Map.class);
  }

//  Map<String, String> getEnv() {
//    Map<String, String> configMap = new HashMap<>();
//    configMap.putIfAbsent("PROPERTIES_BOOTSTRAP_SERVERS", getenv("KAFKA_BOOTSTRAP_SERVERS"));
//    configMap.putIfAbsent("PROPERTIES_GROUP_ID", "mygroupid");
////    configMap.putIfAbsent("JDBC_URL", getenv("JDBC_URL"));
////    configMap.putIfAbsent("JDBC_USERNAME", getenv("JDBC_USERNAME"));
////    configMap.putIfAbsent("JDBC_PASSWORD", getenv("JDBC_PASSWORD"));
//    configMap.putIfAbsent("DATA_PATH", build.resolve("deploy/flink/data").toString());
//    configMap.putIfAbsent("PGHOST", getenv("PGHOST"));
//    configMap.putIfAbsent("PGUSER", getenv("PGUSER"));
//    configMap.putIfAbsent("PGPORT", getenv("PGPORT"));
//    configMap.putIfAbsent("PGPASSWORD", getenv("PGPASSWORD"));
//    configMap.putIfAbsent("PGDATABASE", getenv("PGDATABASE"));

//    return configMap;
//  }

  public String replaceWithEnv(String command) {
    Map<String, String> envVariables = env;
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
  public void initKafka() {
    if (path.resolve("kafka.json").toFile().exists()) {
      Map<String, Object> map = objectMapper.readValue(path.resolve("kafka.json").toFile(), Map.class);
      List<Map<String, Object>> topics = (List<Map<String, Object>>) map.get("topics");

      Properties props = new Properties();
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getenv("PROPERTIES_BOOTSTRAP_SERVERS"));
      try (AdminClient adminClient = AdminClient.create(props)) {
        for (Map<String, Object> topic : topics) {
          NewTopic newTopic = new NewTopic((String) topic.get("name"), 1, (short) 1);
          adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        }
      }
    }
  }

  @SneakyThrows
  public void initPostgres() {
    if (path.resolve("postgres.json").toFile().exists()) {
      Map<String, Object> map = objectMapper.readValue(path.resolve("postgres.json").toFile(), Map.class);
      List<Map<String, Object>> ddl = (List<Map<String, Object>>) map.get("ddl");

      //todo env + default
      String format = String.format("jdbc:postgresql://%s:%s/%s",
          getenv("PGHOST"), getenv("PGPORT"), getenv("PGDATABASE"));
      try (Connection connection = DriverManager.getConnection(format, getenv("PGUSER"), getenv("PGPASSWORD"))) {
        for (Map<String, Object> statement : ddl) {
          String sql = (String) statement.get("sql");
          connection.createStatement().execute(sql);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private String getenv(String key) {
    return this.env.get(key);
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
    Map<String, Object> json = objectMapper.readValue(resource, Map.class);
    JsonObject config = new JsonObject(json);

    ServerConfig serverConfig = new ServerConfig(config);

    // Set Postgres connection options from environment variables
    serverConfig.getPgConnectOptions()
        .setHost(getenv("PGHOST"))
        .setPort(Integer.parseInt(getenv("PGPORT")))
        .setUser(getenv("PGUSER"))
        .setPassword(getenv("PGPASSWORD"))
        .setDatabase(getenv("PGDATABASE"));

    GraphQLServer server = new GraphQLServer(rootGraphqlModel, serverConfig,
        NameCanonicalizer.SYSTEM, getSnowflakeUrl()) {
      @Override
      public String getEnvironmentVariable(String envVar) {
        return getenv(envVar);
      }
    };

    PrometheusMeterRegistry prometheusMeterRegistry = new PrometheusMeterRegistry(
        PrometheusConfig.DEFAULT);
    MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
        .setMicrometerRegistry(prometheusMeterRegistry)
        .setEnabled(true);

    vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

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