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

import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.flinkrunner.SqrlRunner;
import com.datasqrl.flinkrunner.utils.EnvUtils;
import com.datasqrl.flinkrunner.utils.EnvVarResolver;
import com.datasqrl.server.HttpServerVerticle;
import com.datasqrl.server.config.ServerConfigUtil;
import com.datasqrl.server.graphql.ModelContainer;
import com.datasqrl.util.CmdUtils;
import com.datasqrl.util.JsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsFactory;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.table.api.TableResult;

@Slf4j
public class DatasqrlRun {

  private static final int VERTX_DEPLOY_TIMEOUT_SEC = 30;

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
    mapper = JsonUtils.getMapperWithEnvVarResolver(env);
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
    CmdUtils.initializePostgres(planDir, env);
    CmdUtils.initializeKafka(planDir, env);

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
    try {
      closeFlinkJobIfNeeded(
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
    } finally {
      closeVertxAndShutdown();
    }
  }

  public void drainAndCancel() {
    log.debug("Flink cancel initiated...");
    closeFlinkJobIfNeeded(
        () -> {
          var jobClient = tableResult.getJobClient().get();
          Path tmpSavepointDir = null;
          try {
            // Flink exposes drain semantics only through stop-with-savepoint, so keeping it
            // temporary
            tmpSavepointDir = Files.createTempDirectory("sqrl-test-drain-savepoint-");
            jobClient
                .stopWithSavepoint(
                    true, tmpSavepointDir.toAbsolutePath().toString(), SavepointFormatType.NATIVE)
                .get();
          } catch (Exception e) {
            log.warn("Failed to drain Flink job before cancellation. Falling back to cancel.", e);
            try {
              jobClient.cancel().get();
            } catch (Exception cancelException) {
              log.debug("Flink job cancellation failed.", cancelException);
            }
          } finally {
            if (tmpSavepointDir != null) {
              try {
                FileUtils.deleteDirectory(tmpSavepointDir.toFile());
              } catch (IOException e) {
                log.warn(
                    "Failed to delete temporary Flink drain savepoint: {}", tmpSavepointDir, e);
              }
            }
          }
        });
  }

  public void closeVertxAndShutdown() {
    try {
      if (vertx != null) {
        vertx.close().toCompletionStage().toCompletableFuture().get();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted while waiting for Vert.x to stop", e);
    } catch (ExecutionException e) {
      log.error("Failed to stop Vert.x cleanly", e.getCause());
    } finally {
      // Signal shutdown to release the hold only after Vert.x has stopped.
      if (shutdownLatch != null) {
        shutdownLatch.countDown();
      }
    }
  }

  private void closeFlinkJobIfNeeded(Runnable closeFlinkJob) {
    if (tableResult == null) {
      return;
    }

    try {
      var status = tableResult.getJobClient().get().getJobStatus().get();
      if (!status.isGloballyTerminalState()) {
        closeFlinkJob.run();
      }
    } catch (Exception e) {
      // allow failure if job already ended
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

    var resolver = EnvVarResolver.builder().envVars(EnvUtils.addDeploymentDefaults(env)).build();
    var udfPath = env.get("UDF_PATH");

    getLastSavepoint()
        .ifPresent(
            sp -> {
              log.info("Trying to restore from savepoint: {}", sp);
              flinkConfig.set(StateRecoveryOptions.SAVEPOINT_PATH, sp);
            });

    var runner = new SqrlRunner(execMode, flinkConfig, resolver, sqlFile, planFile, udfPath);

    return runner.run();
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
    var serverVerticle = new HttpServerVerticle(planDir, serverConfig, rootGraphqlModel);
    var prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metricsOptions =
        new MicrometerMetricsFactory(prometheusMeterRegistry).newOptions().setEnabled(true);

    vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

    // Block until the verticle is deployed so any deployment failure is thrown on the calling
    // thread with its real cause, instead of being logged and exiting from a Vert.x event-loop
    // thread (which masked the underlying error).
    try {
      var deploymentId =
          vertx
              .deployVerticle(serverVerticle)
              .toCompletionStage()
              .toCompletableFuture()
              .get(VERTX_DEPLOY_TIMEOUT_SEC, TimeUnit.SECONDS);
      log.info("Vertx deployment succeeded. ID: {}", deploymentId);
    } catch (ExecutionException e) {
      var cause = e.getCause() != null ? e.getCause() : e;
      throw new IllegalStateException("Failed to deploy the GraphQL server verticle", cause);
    } catch (TimeoutException e) {
      throw new IllegalStateException(
          "Timed out waiting for the GraphQL server verticle to deploy", e);
    }
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
