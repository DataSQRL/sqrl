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

import static com.datasqrl.env.EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_JDBC_URL;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PASSWORD;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_USERNAME;

import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.flinkrunner.EnvVarResolver;
import com.datasqrl.flinkrunner.SqrlRunner;
import com.datasqrl.graphql.HttpServerVerticle;
import com.datasqrl.graphql.SqrlObjectMapper;
import com.datasqrl.graphql.config.ServerConfigUtil;
import com.datasqrl.graphql.server.ModelContainer;
import com.datasqrl.util.ConfigLoaderUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsFactory;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
import org.apache.flink.core.fs.FileSystem;
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
  private final ObjectMapper mapper;
  @Nullable private final CountDownLatch shutdownLatch;

  private Vertx vertx;
  private TableResult tableResult;

  private DatasqrlRun(
      Path planDir,
      PackageJson sqrlConfig,
      Configuration flinkConfig,
      Map<String, String> env,
      boolean blocking) {
    this.planDir = planDir;
    this.sqrlConfig = sqrlConfig;
    this.flinkConfig = flinkConfig;
    this.env = env;
    mapper = SqrlObjectMapper.getMapperWithEnvVarResolver(env);
    shutdownLatch = blocking ? new CountDownLatch(1) : null;
  }

  public static DatasqrlRun nonBlocking(
      Path planDir, PackageJson sqrlConfig, Configuration flinkConfig, Map<String, String> env) {
    return new DatasqrlRun(planDir, sqrlConfig, flinkConfig, env, false);
  }

  public static DatasqrlRun blocking(
      Path planDir, PackageJson sqrlConfig, Configuration flinkConfig, Map<String, String> env) {
    return new DatasqrlRun(planDir, sqrlConfig, flinkConfig, env, true);
  }

  public TableResult run() {
    initPostgres();
    initKafka();

    startVertx();
    tableResult = runFlinkJob();

    if (shutdownLatch != null) {
      Runtime.getRuntime().addShutdownHook(new Thread(this::stop, "flink-shutdown"));
      tableResult.print();

      try {
        log.info("Flink job completed. Waiting for shutdown signal to stop containers...");
        shutdownLatch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.info("Interrupted while waiting for shutdown signal");
      }
    }

    return tableResult;
  }

  public void stop() {
    log.debug("Flink stop initiated");
    closeCommon(
        () -> {
          var sp =
              tableResult
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
    log.debug("Flink cancel initiated...");
    closeCommon(() -> tableResult.getJobClient().get().cancel());
  }

  private void closeCommon(Runnable closeFlinkJob) {
    if (tableResult != null) {
      try {
        var status = tableResult.getJobClient().get().getJobStatus().get();
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

    // Signal shutdown to release the hold
    if (shutdownLatch != null) {
      shutdownLatch.countDown();
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

    FileSystem.initialize(flinkConfig, null);
    SqrlRunner runner = new SqrlRunner(execMode, flinkConfig, resolver, sqlFile, planFile, udfPath);

    return runner.run();
  }

  @SneakyThrows
  private void initKafka() {
    var kafkaPlanOpt = ConfigLoaderUtils.loadKafkaPhysicalPlan(planDir);
    if (kafkaPlanOpt.isEmpty() || kafkaPlanOpt.get().isEmpty()) {
      log.debug("The Kafka physical plan is empty, skip init");
      return;
    }

    var kafkaPlan = kafkaPlanOpt.get();
    var topicsToCreate = new HashSet<String>();

    Stream.concat(kafkaPlan.topics().stream(), kafkaPlan.testRunnerTopics().stream())
        .map(com.datasqrl.engine.log.kafka.NewTopic::getTopicName)
        .forEach(topicsToCreate::add);

    var bootstrapServers = getenv(KAFKA_BOOTSTRAP_SERVERS);
    if (bootstrapServers == null) {
      throw new IllegalStateException(
          "Failed to get Kafka 'bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS is not set");
    }

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient adminClient = AdminClient.create(props)) {
      Set<String> existingTopics = adminClient.listTopics().names().get();
      for (String topicName : topicsToCreate) {
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
    var postgresPlanOpt = ConfigLoaderUtils.loadPostgresPhysicalPlan(planDir);
    if (postgresPlanOpt.isEmpty() || postgresPlanOpt.get().statements().isEmpty()) {
      log.debug("The Postgres physical plan is empty, skip init");
      return;
    }

    var statements = postgresPlanOpt.get().statements();
    try (Connection connection =
        DriverManager.getConnection(
            getenv(POSTGRES_JDBC_URL), getenv(POSTGRES_USERNAME), getenv(POSTGRES_PASSWORD))) {
      for (var jdbcStmt : statements) {
        log.info("Executing statement {} of type {}", jdbcStmt.getName(), jdbcStmt.getType());
        try (Statement stmt = connection.createStatement()) {
          stmt.execute(jdbcStmt.getSql());
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
    var vertxJson = planDir.resolve("vertx.json").toFile();
    if (!vertxJson.exists()) {
      return;
    }

    var rootGraphqlModel = mapper.readValue(vertxJson, ModelContainer.class).models;
    if (rootGraphqlModel == null || rootGraphqlModel.isEmpty()) {
      return; // no graphql server queries
    }

    var vertxConfigJson = planDir.resolve("vertx-config.json").toFile();
    if (!vertxConfigJson.exists()) {
      throw new IllegalStateException(
          "Server config JSON '%s' does not exist".formatted(vertxConfigJson));
    }

    Map<String, Object> json = mapper.readValue(vertxConfigJson, Map.class);
    var baseServerConfig = ServerConfigUtil.fromConfigMap(json);

    var serverConfig = ServerConfigUtil.mergeConfigs(baseServerConfig, vertxConfig());
    var serverVerticle = new HttpServerVerticle(serverConfig, rootGraphqlModel);
    var prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metricsOptions =
        new MicrometerMetricsFactory(prometheusMeterRegistry).newOptions().setEnabled(true);

    vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));
    vertx
        .deployVerticle(serverVerticle)
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
    if (StringUtils.isBlank(savepointDir)) {
      return Optional.empty();
    }

    log.debug("Using savepoint dir from Flink configuration YAML: {}", savepointDir);
    Path savepointDirPath;
    try {
      savepointDirPath = Paths.get(URI.create(savepointDir));
    } catch (IllegalArgumentException ignored) {
      savepointDirPath = Paths.get(savepointDir);
    }

    if (!Files.isDirectory(savepointDirPath)) {
      log.warn(
          "Savepoint dir '%s' was provided in the Flink config, but it does not exist, will ignore.");
      return Optional.empty();
    }

    try (var files = Files.list(savepointDirPath)) {
      return files
          .filter(Files::isDirectory)
          .map(this::attachCreationTime)
          .max(Comparator.comparing(t -> t.f1))
          .map(t -> t.f0)
          .map(Path::toAbsolutePath)
          .map(Path::toString);
    }
  }

  @SneakyThrows
  private Tuple2<Path, FileTime> attachCreationTime(Path path) {
    BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
    return Tuple2.of(path, attrs.creationTime());
  }

  private Map<String, Object> vertxConfig() {
    return sqrlConfig
        .getEngines()
        .getEngineConfig(VertxEngineFactory.ENGINE_NAME)
        .map(PackageJson.EngineConfig::getConfig)
        .orElse(null);
  }
}
