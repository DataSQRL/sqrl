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
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.ConfigLoaderUtils.StatementInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsFactory;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.table.api.TableResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

@Slf4j
public class DatasqrlRun {

  private final Path planDir;
  private final PackageJson sqrlConfig;
  private final Configuration flinkConfig;
  private final Map<String, String> env;
  private final ObjectMapper objectMapper;

  private Vertx vertx;
  private TableResult execute;

  public DatasqrlRun(
      Path planDir, PackageJson sqrlConfig, Configuration flinkConfig, Map<String, String> env) {
    this.planDir = planDir;
    this.sqrlConfig = sqrlConfig;
    this.flinkConfig = flinkConfig;
    this.env = env;
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
    var execMode = flinkConfig.get(ExecutionOptions.RUNTIME_MODE);
    var isCompiledPlan = sqrlConfig.getCompilerConfig().compileFlinkPlan();

    String sqlFile = null;
    String planFile = null;
    if (execMode == RuntimeExecutionMode.STREAMING && isCompiledPlan) {
      planFile = planDir.resolve("flink-compiled-plan.json").toAbsolutePath().toString();
    } else {
      sqlFile = planDir.resolve("flink-sql.sql").toAbsolutePath().toString();
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
    var topics = ConfigLoaderUtils.loadKafkaTopics(planDir);
    if (topics.isEmpty()) {
      log.warn(
          "The Kafka physical plan does not contain any topics, 'test-runner' topics will be ignored");
      return;
    }

    var mutableTopics = new ArrayList<>(topics);
    mutableTopics.addAll(sqrlConfig.getTestConfig().getCreateTopics());

    var bootstrapServers = getenv("KAFKA_BOOTSTRAP_SERVERS");
    if (bootstrapServers == null) {
      throw new IllegalStateException(
          "Failed to get Kafka 'bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS is not set");
    }

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient adminClient = AdminClient.create(props)) {
      Set<String> existingTopics = adminClient.listTopics().names().get();
      for (String topicName : mutableTopics) {
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
    var statements = ConfigLoaderUtils.loadPostgresStatements(planDir);
    if (statements.isEmpty()) {
      return;
    }

    try (Connection connection =
        DriverManager.getConnection(
            getenv("POSTGRES_JDBC_URL"),
            getenv("POSTGRES_USERNAME"),
            getenv("POSTGRES_PASSWORD"))) {
      for (StatementInfo statement : statements) {
        log.info("Executing statement {} of type {}", statement.name(), statement.type());
        try (Statement stmt = connection.createStatement()) {
          stmt.execute(statement.sql());
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
    if (!planDir.resolve("vertx.json").toFile().exists()) {
      return;
    }
    RootGraphqlModel rootGraphqlModel =
        objectMapper.readValue(planDir.resolve("vertx.json").toFile(), ModelContainer.class).model;
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
    if (planDir.resolve("postgres.json").toFile().exists()) {
      serverConfig
          .getPgConnectOptions()
          .setHost(getenv("POSTGRES_HOST"))
          .setPort(Integer.parseInt(getenv("POSTGRES_PORT")))
          .setUser(getenv("POSTGRES_USERNAME"))
          .setPassword(getenv("POSTGRES_PASSWORD"))
          .setDatabase(getenv("POSTGRES_DATABASE"));
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
                log.info("Vertx deployment ID: {}", res.result());
              } else {
                log.warn("Vertx deployment failed", res.cause());
              }
            });
  }

  @SneakyThrows
  Optional<String> getLastSavepoint() {
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

  public static class ModelContainer {
    public RootGraphqlModel model;
  }
}
