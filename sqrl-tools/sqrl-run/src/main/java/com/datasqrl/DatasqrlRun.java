/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl;

import com.datasqrl.flinkrunner.EnvVarResolver;
import com.datasqrl.flinkrunner.SqrlRunner;
import com.datasqrl.graphql.GraphQLServer;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Resources;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.micrometer.MicrometerMetricsFactory;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public class DatasqrlRun {

  private final Map<String, String> env;
  // Fix override
  Path build = Path.of(System.getProperty("user.dir")).resolve("build");
  Path planPath = build.resolve("deploy").resolve("plan");

  ObjectMapper objectMapper = new ObjectMapper();

  Vertx vertx;
  TableResult execute;

  public static void main(String[] args) {
    var run = new DatasqrlRun();
    run.run(true);
  }

  public DatasqrlRun() {
    this.env = System.getenv();
  }

  public DatasqrlRun(Path planPath, Map<String, String> env) {
    Map<String, String> newEnv = new HashMap<>();
    newEnv.putAll(System.getenv());
    newEnv.putAll(env);
    this.env = newEnv;
    setPlanPath(planPath);
  }

  @VisibleForTesting
  public void setPlanPath(Path planPath) {
    this.planPath = planPath;
    this.build = planPath.getParent().getParent();
  }

  public TableResult run(boolean hold) {
    initPostgres();
    initKafka();

    startVertx();
    execute = runFlinkJob();
    if (hold) {
      execute.print();
    }
    return execute;
  }

  public void stop() {
    if (execute != null) {
      try {
        var status = execute.getJobClient().get().getJobStatus().get();
        if (status != JobStatus.FINISHED) {
          execute.getJobClient().get().cancel();
        }
      } catch (Exception e) {
        // allow failure if job already ended
        //        e.printStackTrace();
      }
    }
    if (vertx != null) {
      vertx.close();
    }
  }

  @SneakyThrows
  public TableResult runFlinkJob() {
    // Read conf if present
    Path packageJson = build.resolve("package.json");
    Map<String, String> config = new HashMap<>();
    RuntimeExecutionMode execMode = RuntimeExecutionMode.STREAMING;
    boolean isCompilePlan = true;
    if (packageJson.toFile().exists()) {
      Map packageJsonMap = getPackageJson();
      Object engines = packageJsonMap.get("engines");
      if (engines != null) {
        Object flink = ((Map) engines).get("flink");
        if (flink != null) {
          if (String.valueOf(((Map) flink).get("mode")).equalsIgnoreCase("batch")) {
            execMode = RuntimeExecutionMode.BATCH;
          }
          Object c = ((Map) flink).get("config");
          if (c instanceof Map m) {
            config.putAll(m);
          }
        }
      }
      Object compiler = packageJsonMap.get("compiler");
      if (compiler != null) {
        isCompilePlan =
            Optional.ofNullable(((Map) compiler).get("compilePlan"))
                .map(Boolean.class::cast)
                .orElse(true);
      }
      Object o = packageJsonMap.get("values");
      if (o instanceof Map map) {
        Object c = map.get("flink-config");
        if (c instanceof Map m) {
          config.putAll(m);
        }
      }
    }

    config.putIfAbsent("taskmanager.memory.network.max", "800m");
    config.putIfAbsent("state.backend.type", "rocksdb");
    config.putIfAbsent("table.exec.resource.default-parallelism", "1");
    config.putIfAbsent("rest.address", "localhost");
    config.putIfAbsent("rest.port", "8081");
    config.putIfAbsent("execution.target", "local"); // mini cluster
    config.putIfAbsent("execution.attached", "true"); // mini cluster
    if (execMode == RuntimeExecutionMode.STREAMING) {
      config.putIfAbsent("table.exec.source.idle-timeout", "1 s");
      config.putIfAbsent("execution.checkpointing.interval", "30 s");
      config.putIfAbsent("execution.checkpointing.min-pause", "20 s");
    }

    // Exposed for tests
    if (env.get("FLINK_RESTART_STRATEGY") != null) {
      config.putIfAbsent("restart-strategy.type", "fixed-delay");
      config.putIfAbsent("restart-strategy.fixed-delay.attempts", "0");
      config.putIfAbsent("restart-strategy.fixed-delay.delay", "5 s");
    }

    String sqlFile = null;
    String planFile = null;
    if (execMode == RuntimeExecutionMode.STREAMING && isCompilePlan) {
      planFile = planPath.resolve("flink-compiled-plan.json").toAbsolutePath().toString();
    } else {
      sqlFile = planPath.resolve("flink-sql.sql").toAbsolutePath().toString();
    }

    var flinkConfig = Configuration.fromMap(config);
    var resolver = new EnvVarResolver(env);
    var udfPath = env.get("UDF_PATH");

    SqrlRunner runner = new SqrlRunner(execMode, flinkConfig, resolver, sqlFile, planFile, udfPath);

    return runner.run();
  }

  @SneakyThrows
  protected Map getPackageJson() {
    return objectMapper.readValue(build.resolve("package.json").toFile(), Map.class);
  }

  @SneakyThrows
  public void initKafka() {
    if (!planPath.resolve("kafka.json").toFile().exists()) {
      return;
    }
    Map<String, Object> map =
        objectMapper.readValue(planPath.resolve("kafka.json").toFile(), Map.class);
    List<Map<String, Object>> topics = (List<Map<String, Object>>) map.get("topics");

    if (topics == null) {
      return;
    }

    List<Map<String, Object>> mutableTopics = new ArrayList<>(topics);

    Object o = getPackageJson().get("values");
    if (o instanceof Map vals) {
      Object o1 = vals.get("create-topics");
      if (o1 instanceof List topicList) {
        for (Object t : topicList) {
          if (t instanceof String string) {
            mutableTopics.add(Map.of("topicName", string));
          }
        }
      }
    }

    Properties props = new Properties();
    if (getenv("PROPERTIES_BOOTSTRAP_SERVERS") == null) {
      throw new RuntimeException("${PROPERTIES_BOOTSTRAP_SERVERS} is not set");
    }
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getenv("PROPERTIES_BOOTSTRAP_SERVERS"));
    try (AdminClient adminClient = AdminClient.create(props)) {
      Set<String> existingTopics = adminClient.listTopics().names().get();

      Set<String> requiredTopics =
          mutableTopics.stream()
              .map(topic -> (String) topic.get("topicName"))
              .collect(Collectors.toSet());
      for (String topicName : requiredTopics) {
        if (existingTopics.contains(topicName)) {
          continue;
        }
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);
        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
      }
    }
  }

  @SneakyThrows
  public void initPostgres() {
    File file = planPath.resolve("postgres.json").toFile();
    if (!file.exists()) {
      return;
    }
    Map plan = objectMapper.readValue(file, Map.class);

    String fullPostgresJDBCUrl =
        "jdbc:postgresql://%s:%s/%s"
            .formatted(getenv("PGHOST"), getenv("PGPORT"), getenv("PGDATABASE"));
    try (Connection connection =
        DriverManager.getConnection(fullPostgresJDBCUrl, getenv("PGUSER"), getenv("PGPASSWORD"))) {
      for (Map statement : (List<Map>) plan.get("statements")) {
        log.info("Executing statement {} of type {}", statement.get("name"), statement.get("type"));
        try (Statement stmt = connection.createStatement()) {
          stmt.execute((String) statement.get("sql"));
        } catch (Exception e) {
          e.printStackTrace();
          assert false : e.getMessage();
        }
      }
    }
  }

  private String getenv(String key) {
    return this.env.get(key);
  }

  @SneakyThrows
  public void startVertx() {
    if (!planPath.resolve("vertx.json").toFile().exists()) {
      return;
    }
    RootGraphqlModel rootGraphqlModel =
        objectMapper.readValue(planPath.resolve("vertx.json").toFile(), ModelContainer.class).model;
    if (rootGraphqlModel == null) {
      return; // no graphql server queries
    }

    URL resource = Resources.getResource("server-config.json");
    Map<String, Object> json = objectMapper.readValue(resource, Map.class);
    JsonObject config = new JsonObject(json);

    ServerConfig serverConfig =
        new ServerConfig(config) {
          @Override
          public String getEnvironmentVariable(String envVar) {
            return getenv(envVar);
          }
        };

    // Set Postgres connection options from environment variables
    if (planPath.resolve("postgres.json").toFile().exists()) {
      serverConfig
          .getPgConnectOptions()
          .setHost(getenv("PGHOST"))
          .setPort(Integer.parseInt(getenv("PGPORT")))
          .setUser(getenv("PGUSER"))
          .setPassword(getenv("PGPASSWORD"))
          .setDatabase(getenv("PGDATABASE"));
    }

    var auth = readAuthentication();
    if (auth != null) {
      serverConfig.setJwtAuth(auth);
    }

    var server = new GraphQLServer(rootGraphqlModel, serverConfig, getSnowflakeUrl());

    var prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metricsOptions =
        new MicrometerMetricsFactory(prometheusMeterRegistry).newOptions().setEnabled(true);

    vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

    vertx
        .deployVerticle(server)
        .onComplete(
            res -> {
              if (res.succeeded()) {
                System.out.println("Deployment id is: " + res.result());
              } else {
                System.out.println("Deployment failed!");
              }
            });
  }

  private JWTAuthOptions readAuthentication() {
    var packageJson = getPackageJson();
    var engines = (Map) packageJson.get("engines");
    if (engines == null) {
      return null;
    }

    var vertx = (Map) engines.get("vertx");
    if (vertx == null) {
      return null;
    }

    var authentication = (Map) vertx.get("authentication");
    if (authentication == null) {
      return null;
    }

    return objectMapper.convertValue(authentication, JWTAuthOptions.class);
  }

  public Optional<String> getSnowflakeUrl() {
    var engines = (Map) getPackageJson().get("engines");
    var snowflake = (Map) engines.get("snowflake");
    if (snowflake != null) {
      var url = snowflake.get("url");
      if (url instanceof String string) {
        return Optional.of(string);
      }
    }

    return Optional.empty();
  }

  public static class ModelContainer {
    public RootGraphqlModel model;
  }
}
