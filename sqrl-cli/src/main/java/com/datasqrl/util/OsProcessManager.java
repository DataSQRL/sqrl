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
package com.datasqrl.util;

import static com.datasqrl.env.EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS;
import static com.datasqrl.env.EnvVariableNames.KAFKA_GROUP_ID;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_AUTHORITY;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_DATABASE;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_HOST;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_JDBC_URL;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PASSWORD;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PORT;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_USERNAME;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_VERSION;

import com.datasqrl.env.GlobalEnvironmentStore;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/** Manages operating system process interactions for the CLI. */
@Slf4j
public class OsProcessManager {

  private static final String DEFAULT_POSTGRES_VERSION = "17";
  private static final int SERVICE_TIMEOUT_IN_SEC = 15;

  private static final String POSTGRES_DATA_PATH = "/data/postgres";
  private static final String REDPANDA_DATA_PATH = "/data/redpanda";
  private static final String FLINK_CP_DATA_PATH = "/data/flink/checkpoints";
  private static final String FLINK_SP_DATA_PATH = "/data/flink/savepoints";
  private static final String LOGS_PATH = "/tmp/logs";

  private static final String LOCALHOST = "localhost";
  private static final String RP_PORT = "9092";
  private static final String PG_PORT = "5432";
  private static final String PG_DB = "datasqrl";
  private static final String PG_AUTHORITY = LOCALHOST + ':' + PG_PORT + '/' + PG_DB;

  private final Map<String, String> env;
  private final String ownerUser;
  private final String ownerGroup;

  public OsProcessManager(Map<String, String> env) {
    this.env = new HashMap<>(env);
    ownerUser = env.getOrDefault("BUILD_UID", "root");
    ownerGroup = env.getOrDefault("BUILD_GID", "root");
  }

  /**
   * Analyzes the execution plan directory and starts only the dependent services that are actually
   * required for the DataSQRL execution. This method dynamically determines whether Kafka (via
   * Redpanda) and/or PostgreSQL services need to be started based on the presence of relevant
   * configurations in the plan directory.
   *
   * @param planDir the path to the directory containing the execution plan artifacts, including
   *     Kafka topic configurations and PostgreSQL statements
   * @throws IllegalStateException if any required service fails to start within the timeout period
   *     or encounters initialization errors
   */
  public void startDependentServices(Path planDir) {
    var kafkaPlanned = isKafkaPlanned(planDir);
    var postgresPlanned = isPostgresPlanned(planDir);

    try {
      startRedpanda(kafkaPlanned);
      startPostgres(postgresPlanned);
      createDirectories();

      // Add every registered env entry to global environment store
      GlobalEnvironmentStore.putAll(env);

      log.info("All necessary services are up");
    } catch (Exception e) {
      log.error("Failed to start services", e);
      throw new IllegalStateException("Service startup failed", e);
    }
  }

  /**
   * Performs cleanup operations after DataSQRL execution, including moving log files to the build
   * directory and setting proper file ownership.
   *
   * @param buildDir the build directory where logs should be moved and ownership should be set
   * @throws Exception if log file movement fails or ownership setting encounters errors
   */
  public void teardown(Path buildDir) throws Exception {
    Path source = Paths.get(LOGS_PATH);
    Path target = buildDir.resolve("logs");

    if (Files.exists(source)) {
      FileUtils.copyDirectory(source.toFile(), target.toFile());
    }

    setOwnerForDir(buildDir.getParent());
  }

  /**
   * Sets the ownership of a directory and all its contents recursively using the configured
   * BUILD_UID and BUILD_GID environment variables.
   *
   * <p>If the BUILD_UID or BUILD_GID environment variables are not set or are blank, this method
   * performs no operation. If the {@code chown} command fails, a warning is logged but no exception
   * is thrown.
   *
   * @param dir the directory whose ownership should be set recursively
   * @throws IOException if there's an error starting the {@code chown} process
   * @throws InterruptedException if the current thread is interrupted while waiting for the chown
   *     process to complete
   */
  public void setOwnerForDir(Path dir) throws IOException, InterruptedException {
    if (StringUtils.isNoneBlank(ownerUser, ownerUser)) {
      var owner = ownerUser + ':' + ownerGroup;
      var absPath = dir.toAbsolutePath().toString();
      var pb = initProcessBuilder("chown", "-R", owner, absPath);
      var proc = pb.start();
      if (proc.waitFor() != 0) {
        log.warn("Failed to set owner '{}' for directory: {}", owner, absPath);
      }
    }
  }

  private void createDirectories() throws IOException, InterruptedException {
    log.debug("Creating necessary directories ...");

    createDirectoryWithPermission(Paths.get(LOGS_PATH));

    Files.createDirectories(Paths.get(FLINK_CP_DATA_PATH));
    Files.createDirectories(Paths.get(FLINK_SP_DATA_PATH));
  }

  private void createDirectoryWithPermission(Path dir) throws IOException, InterruptedException {
    Files.createDirectories(dir);
    setOwnerForDir(dir);
  }

  private void startRedpanda(boolean kafkaPlanned) throws IOException, InterruptedException {
    if (!kafkaPlanned) {
      log.debug("Skip starting Redpanda, as plan has no relevant Kafka parts");
      return;
    }

    String bootstrapServers;
    var externalBootstrapServers = env.get(KAFKA_BOOTSTRAP_SERVERS);
    if (externalBootstrapServers != null) {
      log.info(
          "Skip starting Redpanda, because {}={} is provided",
          KAFKA_BOOTSTRAP_SERVERS,
          externalBootstrapServers);
      bootstrapServers = externalBootstrapServers;

    } else {
      log.info("Starting Redpanda ...");

      var redpandaDataPath = Paths.get(REDPANDA_DATA_PATH);
      Files.createDirectories(redpandaDataPath);

      // Start Redpanda process
      ProcessBuilder pb =
          new ProcessBuilder(
              "rpk",
              "redpanda",
              "start",
              "--schema-registry-addr",
              "0.0.0.0:8086",
              "--overprovisioned",
              "--config",
              "/etc/redpanda/redpanda.yaml",
              "--smp",
              "1",
              "--memory",
              "1G",
              "--reserve-memory",
              "0M",
              "--node-id",
              "0",
              "--check=false");

      pb.redirectOutput(Paths.get(LOGS_PATH, "redpanda.log").toFile());
      pb.redirectErrorStream(true);

      var redpandaProcess = pb.start();

      // Wait a moment for the process to initialize
      Thread.sleep(2000);

      // Check if the process is still alive
      if (!redpandaProcess.isAlive()) {
        var exitCode = redpandaProcess.exitValue();
        var errorDetails = readServiceLogFile("Redpanda");
        throw new IllegalStateException(
            String.format(
                "Redpanda process failed to start (exit code: %d). Error details: %s",
                exitCode, errorDetails));
      }

      waitForService("Redpanda", RP_PORT, "rpk", "cluster", "health");

      bootstrapServers = LOCALHOST + ':' + RP_PORT;
      log.info("Redpanda started successfully");
    }

    // Set environment variables
    setEnvironmentVariable(KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);
    setEnvironmentVariable(KAFKA_GROUP_ID, UUID.randomUUID().toString());
  }

  private void startPostgres(boolean postgresPlanned) throws IOException, InterruptedException {
    if (!postgresPlanned) {
      log.debug("Skip starting Postgres, as plan has no relevant JDBC parts");
      return;
    }

    var postgresDataPath = Paths.get(POSTGRES_DATA_PATH);
    var started = false;

    // Create Postgres dir if necessary
    if (!Files.exists(postgresDataPath)) {
      Files.createDirectories(postgresDataPath);
      executePostgresCommand("chown", "-R", "postgres:postgres", POSTGRES_DATA_PATH);
    }

    if (isDirectoryEmpty(postgresDataPath)) {
      log.info("Initializing Postgres database ...");

      var postgresVersion = env.getOrDefault(POSTGRES_VERSION, DEFAULT_POSTGRES_VERSION);
      executePostgresCommand(
          "su",
          "-",
          "postgres",
          "-c",
          String.format(
              "/usr/lib/postgresql/%s/bin/initdb -D %s", postgresVersion, POSTGRES_DATA_PATH));

      startPostgresService();
      started = true;

      // Create user and database
      executePostgresCommand(
          "su",
          "-",
          "postgres",
          "-c",
          "psql -U postgres -c \"ALTER USER postgres WITH PASSWORD 'postgres';\"");
      executePostgresCommand(
          "su", "-", "postgres", "-c", "psql -U postgres -c \"CREATE DATABASE datasqrl;\"");
      executePostgresCommand(
          "su", "-", "postgres", "-c", "psql -U postgres -c \"CREATE EXTENSION vector;\"");
    }

    if (!started) {
      log.info("Starting Postgres service ...");
      startPostgresService();
    }

    // Set environment variables
    setEnvironmentVariable(POSTGRES_HOST, LOCALHOST);
    setEnvironmentVariable(POSTGRES_PORT, PG_PORT);
    setEnvironmentVariable(POSTGRES_DATABASE, PG_DB);
    setEnvironmentVariable(POSTGRES_AUTHORITY, PG_AUTHORITY);
    setEnvironmentVariable(POSTGRES_JDBC_URL, "jdbc:postgresql://" + PG_AUTHORITY);
    setEnvironmentVariable(POSTGRES_USERNAME, "postgres");
    setEnvironmentVariable(POSTGRES_PASSWORD, "postgres");

    log.info("Postgres started successfully");
  }

  private boolean isKafkaPlanned(Path planDir) {
    var kafkaPlan = ConfigLoaderUtils.loadKafkaPhysicalPlan(planDir);

    return kafkaPlan.isPresent() && !kafkaPlan.get().isEmpty();
  }

  private boolean isPostgresPlanned(Path planDir) {
    var jdbcPlan = ConfigLoaderUtils.loadPostgresPhysicalPlan(planDir);
    return jdbcPlan.isPresent() && !jdbcPlan.get().statements().isEmpty();
  }

  private void startPostgresService() throws IOException, InterruptedException {
    executePostgresCommand("service", "postgresql", "start");
    waitForService("Postgres", PG_PORT, "pg_isready", "-h", LOCALHOST, "-p", PG_PORT);
  }

  private boolean isDirectoryEmpty(Path path) throws IOException {
    if (!Files.exists(path)) {
      return true;
    }
    try (var stream = Files.list(path)) {
      return stream.findFirst().isEmpty();
    }
  }

  private void executePostgresCommand(String... command) throws IOException, InterruptedException {
    var pb = new ProcessBuilder(command);
    pb.redirectOutput(
        ProcessBuilder.Redirect.appendTo(Paths.get(LOGS_PATH, "postgres.log").toFile()));
    pb.redirectErrorStream(true);

    var process = pb.start();
    int exitCode = process.waitFor();

    if (exitCode != 0) {
      throw new IllegalStateException(
          "Command failed with exit code " + exitCode + ": " + String.join(" ", command));
    }
  }

  private void waitForService(String serviceName, String port, String... checkCommand) {
    log.info("Waiting for {} to be ready at {}:{} ...", serviceName, LOCALHOST, port);

    ServiceHealthChecker healthChecker =
        () -> {
          try {
            var pb = initProcessBuilder(checkCommand);
            var proc = pb.start();
            return proc.waitFor() == 0;

          } catch (Exception e) {
            return false;
          }
        };

    long startTime = System.currentTimeMillis();
    long timeoutMs = TimeUnit.SECONDS.toMillis(SERVICE_TIMEOUT_IN_SEC);

    while (System.currentTimeMillis() - startTime < timeoutMs) {
      // Check if service is ready
      if (healthChecker.isHealthy()) {
        log.debug("{} is ready!", serviceName);
        return;
      }

      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while waiting for " + serviceName, e);
      }
    }

    throw new IllegalStateException(serviceName + " did not become ready within timeout");
  }

  private ProcessBuilder initProcessBuilder(String... command) {
    var pb = new ProcessBuilder(command);
    pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
    pb.redirectError(ProcessBuilder.Redirect.DISCARD);

    return pb;
  }

  private void setEnvironmentVariable(String key, String value) {
    GlobalEnvironmentStore.put(key, value);
  }

  String readServiceLogFile(String serviceName) {
    String logFileName = serviceName.toLowerCase() + ".log";
    Path logFile = Paths.get(LOGS_PATH, logFileName);
    try {
      if (Files.exists(logFile)) {
        // Read the last 50 lines of the log file for error details
        var lines = Files.readAllLines(logFile);
        int start = Math.max(0, lines.size() - 50);
        return lines.subList(start, lines.size()).stream()
            .reduce("", (acc, line) -> acc + line + "\n");
      } else {
        return "Log file not found at " + logFile;
      }
    } catch (IOException e) {
      return "Failed to read log file: " + e.getMessage();
    }
  }

  @FunctionalInterface
  private interface ServiceHealthChecker {
    boolean isHealthy();
  }
}
