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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
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

  private final Map<String, String> env;
  private final String ownerUser;
  private final String ownerGroup;

  public OsProcessManager(Map<String, String> env) {
    this.env = new HashMap<>(env);
    ownerUser = env.getOrDefault("BUILD_UID", "root");
    ownerGroup = env.getOrDefault("BUILD_GID", "root");
  }

  /**
   * Starts all required services for DataSQRL execution. This includes Postgres, Redpanda, and
   * creates necessary directories.
   */
  public void startDependentServices() {
    try {
      startRedpanda();
      startPostgres();
      createDirectories();

      // Add every registered env entry to sys properties
      env.forEach(System::setProperty);

      log.info("All necessary services are up");
    } catch (Exception e) {
      log.error("Failed to start services", e);
      throw new IllegalStateException("Service startup failed", e);
    }
  }

  /**
   * @param buildDir
   */
  public void teardown(Path buildDir) throws Exception {
    Path target = buildDir.resolve("logs");
    FileUtils.moveDirectory(Paths.get(LOGS_PATH).toFile(), target.toFile());
    setOwnerForDir(buildDir.getParent());
  }

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

  private void startRedpanda() throws IOException, InterruptedException {
    // Start Redpanda if KAFKA_HOST is not set
    if (env.get("KAFKA_HOST") != null) {
      return;
    }

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

    waitForRedpanda("localhost", 9092);

    // Set environment variables
    setSystemProperty("KAFKA_HOST", "localhost");
    setSystemProperty("KAFKA_PORT", "9092");
    setSystemProperty("PROPERTIES_BOOTSTRAP_SERVERS", "localhost:9092");

    log.info("Redpanda started successfully");
  }

  private void startPostgres() throws IOException, InterruptedException {
    var postgresDataPath = Paths.get(POSTGRES_DATA_PATH);
    var started = false;

    // Create Postgres dir if necessary
    if (!Files.exists(postgresDataPath)) {
      Files.createDirectories(postgresDataPath);
      executePostgresCommand("chown", "-R", "postgres:postgres", POSTGRES_DATA_PATH);
    }

    if (isDirectoryEmpty(postgresDataPath)) {
      log.info("Initializing Postgres database ...");

      var postgresVersion = env.getOrDefault("POSTGRES_VERSION", DEFAULT_POSTGRES_VERSION);
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

    // Start Postgres if POSTGRES_HOST is not set
    if (env.get("POSTGRES_HOST") == null) {
      if (!started) {
        log.info("Starting Postgres service ...");
        startPostgresService();
      }

      // Set environment variables
      setSystemProperty("POSTGRES_HOST", "localhost");
      setSystemProperty("POSTGRES_PORT", "5432");
      setSystemProperty("JDBC_URL", "jdbc:postgresql://localhost:5432/datasqrl");
      setSystemProperty("JDBC_AUTHORITY", "localhost:5432/datasqrl");
      setSystemProperty("PGHOST", "localhost");
      setSystemProperty("PGUSER", "postgres");
      setSystemProperty("JDBC_USERNAME", "postgres");
      setSystemProperty("JDBC_PASSWORD", "postgres");
      setSystemProperty("PGPORT", "5432");
      setSystemProperty("PGPASSWORD", "postgres");
      setSystemProperty("PGDATABASE", "datasqrl");
    }

    log.info("Postgres started successfully");
  }

  private void startPostgresService() throws IOException, InterruptedException {
    executePostgresCommand("service", "postgresql", "start");
    waitForPostgres("localhost", 5432);
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

  private void waitForPostgres(String host, int port) {
    waitForService("Postgres", host, port, "pg_isready", "-h", host, "-p", String.valueOf(port));
  }

  private void waitForRedpanda(String host, int port) {
    waitForService("Redpanda", host, port, "rpk", "cluster", "health");
  }

  private void waitForService(String serviceName, String host, int port, String... checkCommand) {
    log.info("Waiting for {} to be ready at {}:{} ...", serviceName, host, port);

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

  private void setSystemProperty(String key, String value) {
    System.setProperty(key, value);
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
