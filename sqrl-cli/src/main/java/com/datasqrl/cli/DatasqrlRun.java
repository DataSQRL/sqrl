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
package com.datasqrl.cli;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.config.PackageJson;
import com.datasqrl.flinkrunner.EnvVarResolver;
import com.datasqrl.flinkrunner.SqrlRunner;
import com.datasqrl.graphql.HttpServerVerticle;
import com.datasqrl.graphql.SqrlObjectMapper;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.config.ServerConfigUtil;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsFactory;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.table.api.TableResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public class DatasqrlRun {

  private final Path planPath;
  private final Path build;
  private final PackageJson sqrlConfig;
  private final Configuration flinkConfig;
  private final Map<String, String> env;
  private final boolean testRun;
  private final ObjectMapper objectMapper;

  private Vertx vertx;
  private TableResult execute;

  public DatasqrlRun(Path planPath, PackageJson sqrlConfig, Configuration flinkConfig) {
    this(planPath, sqrlConfig, flinkConfig, System.getenv(), false);
  }

  public DatasqrlRun(
      Path planPath,
      PackageJson sqrlConfig,
      Configuration flinkConfig,
      Map<String, String> env,
      boolean testRun) {
    this.planPath = planPath;
    this.sqrlConfig = sqrlConfig;
    this.flinkConfig = flinkConfig;
    this.env = env;
    this.testRun = testRun;
    build = planPath.getParent().getParent();
    objectMapper = SqrlObjectMapper.MAPPER;
  }

  public TableResult run(boolean hold, boolean shutdownHook) {
    initPostgres();
    initKafka();

    startVertx();
    execute = runFlinkJob();

    if (shutdownHook) {
      Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "flink-shutdown"));
    }

    if (hold) {
      execute.print();
    }

    return execute;
  }

  public void stop() {
    log.info("Stopping with savepoint initiated");
    closeCommon(
        () -> {
          var sp =
              execute
                  .getJobClient()
                  .get()
                  .stopWithSavepoint(false, null, SavepointFormatType.NATIVE);
          try {
            var spPath = sp.get();
            log.info("Savepoint created at {}", spPath);
          } catch (Exception e) {
            log.error("Savepoint creation failed.", e);
          }
        });
  }

  public void cancel() {
    closeCommon(() -> execute.getJobClient().get().cancel());
  }

  private void closeCommon(Runnable closeFlinkJob) {
    if (execute != null) {
      try {
        var status = execute.getJobClient().get().getJobStatus().get();
        if (status != JobStatus.FINISHED) {
          closeFlinkJob.run();
        }
      } catch (Exception e) {
        // allow failure if job already ended
      }
    }
    if (vertx != null) {
      vertx.close();
    }
  }

  @SneakyThrows
  private TableResult runFlinkJob() {
    applyInternalTestConfig();
    var execMode = flinkConfig.get(ExecutionOptions.RUNTIME_MODE);
    var isCompiledPlan = sqrlConfig.getCompilerConfig().compileFlinkPlan();

    String sqlFile = null;
    String planFile = null;
    if (execMode == RuntimeExecutionMode.STREAMING && isCompiledPlan) {
      planFile = planPath.resolve("flink-compiled-plan.json").toAbsolutePath().toString();
    } else {
      sqlFile = planPath.resolve("flink-sql.sql").toAbsolutePath().toString();
    }

    var resolver = new EnvVarResolver(env);
    var udfPath = env.get("UDF_PATH");

    getLastSavepoint()
        .ifPresent(
            sp -> {
              log.info("Trying to restore from savepoint: {}", sp);
              flinkConfig.set(SavepointConfigOptions.SAVEPOINT_PATH, sp);
            });

    SqrlRunner runner = new SqrlRunner(execMode, flinkConfig, resolver, sqlFile, planFile, udfPath);

    return runner.run();
  }

  @SneakyThrows
  private void initKafka() {
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
    sqrlConfig
        .getTestConfig()
        .getCreateTopics()
        .forEach(topic -> mutableTopics.add(Map.of("topicName", topic)));

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
  private void initPostgres() {
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
  private void startVertx() {
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

    serverConfig = ServerConfigUtil.mergeConfigs(serverConfig, vertxConfig());

    var server = new HttpServerVerticle(serverConfig, rootGraphqlModel);

    var prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metricsOptions =
        new MicrometerMetricsFactory(prometheusMeterRegistry).newOptions().setEnabled(true);

    vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

    vertx
        .deployVerticle(server)
        .onComplete(
            res -> {
              if (res.succeeded()) {
                log.info("Vertx deplotment ID: {}", res.result());
              } else {
                log.warn("Vertx deplotment failed", res.cause());
              }
            });
  }

  @SneakyThrows
  Optional<String> getLastSavepoint() {
    if (testRun) {
      return Optional.empty();
    }

    var savepointDir = flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY);
    if (StringUtils.isNotBlank(savepointDir)) {
      log.debug("Using savepoint dir from Flink configuration YAML: {}", savepointDir);
    } else {
      savepointDir = env.get("FLINK_SP_DATA_PATH");
    }

    if (StringUtils.isNotBlank(savepointDir)) {
      log.debug("Using savepoint dir from FLINK_SP_DATA_PATH env var: {}", savepointDir);
    } else {
      return Optional.empty();
    }

    Path savepointDirPath;
    try {
      savepointDirPath = Paths.get(URI.create(savepointDir));
    } catch (IllegalArgumentException ignored) {
      savepointDirPath = Paths.get(savepointDir);
    }

    checkArgument(
        Files.isDirectory(savepointDirPath),
        String.format("Savepoint dir '%s' does not exist", savepointDir));

    return Files.list(savepointDirPath)
        .filter(Files::isDirectory)
        .map(this::attachCreationTime)
        .max(Comparator.comparing(t -> t.f1))
        .map(t -> t.f0)
        .map(Path::toAbsolutePath)
        .map(Path::toString);
  }

  @SneakyThrows
  private Tuple2<Path, FileTime> attachCreationTime(Path path) {
    BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
    return Tuple2.of(path, attrs.creationTime());
  }

  private Map<String, Object> vertxConfig() {
    return sqrlConfig
        .getEngines()
        .getEngineConfig(EngineIds.SERVER)
        .map(PackageJson.EngineConfig::getConfig)
        .orElse(null);
  }

  void applyInternalTestConfig() {
    if (testRun) {
      flinkConfig.set(DeploymentOptions.TARGET, "local");

      if (env.get("FLINK_RESTART_STRATEGY") != null) {
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        flinkConfig.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 0);
        flinkConfig.set(
            RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(5));
      }
    }
  }

  public static class ModelContainer {
    public RootGraphqlModel model;
  }
}
