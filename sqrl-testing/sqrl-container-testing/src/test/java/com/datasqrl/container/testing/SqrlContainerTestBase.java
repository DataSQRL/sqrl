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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Slf4j
public abstract class SqrlContainerTestBase {

  protected Path testDir;

  protected abstract String getTestCaseName();

  @BeforeEach
  void setupBeforeEach() {
    testDir = itPath(getTestCaseName());
  }

  @AfterEach
  protected void commonTearDown() {
    cleanupContainers();
  }

  protected static final String SQRL_CMD_IMAGE = "datasqrl/cmd";
  protected static final String SQRL_SERVER_IMAGE = "datasqrl/sqrl-server";
  protected static final String BUILD_DIR = "/build";
  protected static final int HTTP_SERVER_PORT = 8888;

  protected static Network sharedNetwork;
  protected static CloseableHttpClient sharedHttpClient;
  protected static final ObjectMapper objectMapper = new ObjectMapper();

  protected GenericContainer<?> cmd;
  protected GenericContainer<?> serverContainer;

  @BeforeAll
  static void setUpSharedResources() {
    sharedNetwork = Network.newNetwork();
    sharedHttpClient = HttpClients.createDefault();
    log.info("Shared test resources initialized");
  }

  @AfterAll
  static void tearDownSharedResources() {
    try {
      if (sharedHttpClient != null) {
        sharedHttpClient.close();
      }
      if (sharedNetwork != null) {
        sharedNetwork.close();
      }
      log.info("Shared test resources cleaned up");
    } catch (Exception e) {
      log.warn("Error during shared resource cleanup", e);
    }
  }

  protected GenericContainer<?> createCmdContainer(Path workingDir) {
    return createCmdContainer(workingDir, true);
  }

  protected GenericContainer<?> createCmdContainer(Path workingDir, boolean debug) {
    assertThat(workingDir).exists().isDirectory();

    var container =
        new GenericContainer<>(DockerImageName.parse(SQRL_CMD_IMAGE + ":" + getImageTag()))
            .withWorkingDirectory(BUILD_DIR)
            .withFileSystemBind(workingDir.toString(), BUILD_DIR, BindMode.READ_WRITE)
            .withEnv("TZ", "America/Los_Angeles");

    if (debug) {
      container = container.withEnv("DEBUG", "1");
    }

    return container;
  }

  @SuppressWarnings("resource")
  protected GenericContainer<?> createServerContainer(Path workingDir) {
    var deployPlanPath = workingDir.resolve("build/deploy/plan");
    assertThat(deployPlanPath).exists().isDirectory();

    return new GenericContainer<>(DockerImageName.parse(SQRL_SERVER_IMAGE + ":" + getImageTag()))
        .withNetwork(sharedNetwork)
        .withExposedPorts(HTTP_SERVER_PORT)
        .withFileSystemBind(deployPlanPath.toString(), "/opt/sqrl/config", BindMode.READ_ONLY)
        .withEnv("DEBUG", "1")
        .waitingFor(
            Wait.forLogMessage(".*GraphQL verticle deployed successfully.*", 1)
                .withStartupTimeout(Duration.ofSeconds(20)));
  }

  protected void compileSqrlScript(String scriptName, Path workingDir) {
    sqrlScript(workingDir, "compile", scriptName);
  }

  protected ContainerResult sqrlScript(Path workingDir, String... command) {
    return sqrlScript(workingDir, true, command);
  }

  protected ContainerResult sqrlScript(Path workingDir, boolean debug, String... command) {
    log.info("Docker run command to reproduce:");
    log.info(getDockerRunCommand(workingDir, SQRL_CMD_IMAGE, getImageTag(), false, command));
    cmd = createCmdContainer(workingDir, debug).withCommand(command);

    cmd.start();

    // Wait for the container to finish running
    await().atMost(Duration.ofMinutes(5)).until(() -> !cmd.isRunning());

    var exitCode = cmd.getCurrentContainerInfo().getState().getExitCodeLong();
    var logs = cmd.getLogs();
    if (exitCode != 0) {
      log.error("SQRL compilation failed with exit code {}\n{}", exitCode, logs);
      throw new ContainerError("SQRL compilation failed", exitCode, logs);
    }

    log.info("SQRL script {} compiled successfully", Arrays.toString(command));
    validatePlan(workingDir, logs);
    assertBuildNotOwnedByRoot(testDir, logs);

    return new ContainerResult(cmd, exitCode, logs);
  }

  protected record ContainerResult(GenericContainer<?> cmd, Long exitCode, String logs) {}

  @Getter
  public static class ContainerError extends RuntimeException {

    private static final long serialVersionUID = -2159257606710389109L;

    private final Long exitCode;
    private final String logs;

    public ContainerError(String message, Long exitCode, String logs, Throwable cause) {
      super(message, cause);
      this.exitCode = exitCode;
      this.logs = logs;
    }

    public ContainerError(String message, Long exitCode, String logs) {
      super(message);
      this.exitCode = exitCode;
      this.logs = logs;
    }
  }

  private void validatePlan(Path workingDir, String logs) {
    var planDir = workingDir.resolve("build/deploy/plan");
    assertThat(planDir).as("Compiler output:\n%s", logs).exists().isDirectory();

    assertSoftly(
        softAssertions -> {
          softAssertions.assertThat(planDir.resolve("flink.json")).exists().isRegularFile();
          softAssertions.assertThat(planDir.resolve("kafka.json")).exists().isRegularFile();
          softAssertions.assertThat(planDir.resolve("postgres.json")).exists().isRegularFile();
          softAssertions.assertThat(planDir.resolve("vertx.json")).exists().isRegularFile();
          softAssertions.assertThat(planDir.resolve("vertx-config.json")).exists().isRegularFile();
        });
  }

  protected void startGraphQLServer(Path workingDir) {
    startGraphQLServer(workingDir, c -> {});
  }

  protected void startGraphQLServer(
      Path workingDir, Consumer<GenericContainer<?>> containerCustomizer) {
    log.info("Docker run command to reproduce:");
    log.info(getDockerRunCommand(workingDir, SQRL_SERVER_IMAGE, getImageTag(), true));
    serverContainer = createServerContainer(workingDir);

    containerCustomizer.accept(serverContainer);

    try {
      serverContainer.start();
      log.info("HTTP server started on port {}", serverContainer.getMappedPort(HTTP_SERVER_PORT));
    } catch (Exception e) {
      log.error("Failed to start GraphQL server container:");
      log.error("Container image: {}", SQRL_SERVER_IMAGE + ":" + getImageTag());
      String logs = null;
      try {
        logs = serverContainer.getLogs();
        log.error("Container logs: {}", logs);
      } catch (Exception logException) {
        log.error("Could not retrieve container logs: {}", logException.getMessage());
      }
      try {
        var containerInfo = serverContainer.getCurrentContainerInfo();
        if (containerInfo != null) {
          log.error("Container state: {}", containerInfo.getState());
        }
      } catch (Exception stateException) {
        log.error("Could not retrieve container state: {}", stateException.getMessage());
      }

      throw new RuntimeException("Failed to start GraphQL server container:\n" + logs, e);
    }
  }

  protected String getBaseUrl() {
    if (serverContainer == null || !serverContainer.isRunning()) {
      throw new IllegalStateException("Server container is not running");
    }
    return "http://localhost:" + serverContainer.getMappedPort(HTTP_SERVER_PORT);
  }

  protected String getGraphQLEndpoint() {
    return getBaseUrl() + "/graphql";
  }

  protected String getMetricsEndpoint() {
    return getBaseUrl() + "/metrics";
  }

  protected String getHealthEndpoint() {
    return getBaseUrl() + "/health";
  }

  protected void verifyNoSlfWarnings(GenericContainer<?> container) {
    var logs = container.getLogs();
    var slf4jWarningPattern = Pattern.compile("(log4j:WARN|SLF4J\\(W\\)|SLF4J:WARN)");

    if (slf4jWarningPattern.matcher(logs).find()) {
      throw new AssertionError("SLF4J / log4j warnings detected in container logs: " + logs);
    }
  }

  protected void verifyLogContains(GenericContainer<?> container, String expectedPattern) {
    var logs = container.getLogs();
    if (!logs.contains(expectedPattern)) {
      throw new AssertionError(
          "Expected log entry '" + expectedPattern + "' not found in: " + logs);
    }
  }

  private String getImageTag() {
    return System.getProperty("docker.image.tag", "local");
  }

  private String getDockerRunCommand(
      Path workingDir, String imageName, String imageTag, boolean isServer, String... commands) {
    var sb = new StringBuilder();
    sb.append("docker run -it --rm");

    if (isServer) {
      var deployPlanPath = workingDir.resolve("build/deploy/plan").toString();
      sb.append(" -p ").append(HTTP_SERVER_PORT).append(":").append(HTTP_SERVER_PORT);
      sb.append(" -v \"").append(deployPlanPath).append(":/opt/sqrl/config\"");
    } else {
      sb.append(" -v \"").append(workingDir).append(":").append(BUILD_DIR).append("\"");
      sb.append(" -w ").append(BUILD_DIR);
      sb.append(" -e TZ=America/Los_Angeles");
    }

    sb.append(" ").append(imageName).append(":").append(imageTag);

    for (String command : commands) {
      sb.append(" ").append(command);
    }

    return sb.toString();
  }

  @SneakyThrows
  protected static Path itPath(String relativePath) {
    var path =
        Paths.get("../sqrl-integration-tests/src/test/resources/usecases", relativePath)
            .toAbsolutePath();
    assertThat(path).exists().isDirectory();
    return path.toRealPath();
  }

  protected static void assertBuildNotOwnedByRoot(Path testDir, String logs) {
    var buildPath = testDir.resolve("build");
    assertThat(buildPath).exists();

    assertOwner(buildPath, logs);
  }

  protected static void assertOwner(Path path, String logs) {
    try {
      var owner = Files.getOwner(path);
      assertThat(owner.getName())
          .as("Build directory should not be owned by root user: %s\n%s", path, logs)
          .isNotEqualTo("root");
      log.debug("Build directory {} is owned by: {}", path, owner.getName());
    } catch (Exception e) {
      fail("Failed to check build directory ownership: " + e.getMessage(), e);
    }
  }

  protected void validateBasicGraphQLResponse(HttpResponse response) throws Exception {
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    var responseBody = EntityUtils.toString(response.getEntity());
    var jsonResponse = objectMapper.readTree(responseBody);

    assertThat(jsonResponse.has("data")).isTrue();
    assertThat(jsonResponse.get("data").has("__typename")).isTrue();
    assertThat(jsonResponse.get("data").get("__typename").asText()).isEqualTo("Query");
  }

  protected void compileAndStartServer(String scriptName, Path testDir) throws Exception {
    compileSqrlScript(scriptName, testDir);
    startGraphQLServer(testDir);
  }

  protected void compileAndStartServer(
      String scriptName, Path testDir, Consumer<GenericContainer<?>> containerCustomizer)
      throws Exception {
    compileSqrlScript(scriptName, testDir);
    startGraphQLServer(testDir, containerCustomizer);
  }

  protected HttpResponse executeGraphQLQuery(String query) throws Exception {
    return executeGraphQLQuery(query, null);
  }

  protected HttpResponse executeGraphQLQuery(String query, String jwtToken) throws Exception {
    var request = new HttpPost(getGraphQLEndpoint());
    request.setEntity(new StringEntity(query, ContentType.APPLICATION_JSON));

    if (jwtToken != null) {
      request.setHeader("Authorization", "Bearer " + jwtToken);
    }

    return sharedHttpClient.execute(request);
  }

  protected void assertLogFiles(String logs, Path testDir) {
    var logsDir = testDir.resolve("build/logs");
    assertThat(logsDir).as("Logs directory should exist\n%s", logs).exists().isDirectory();

    var cliLogFile = logsDir.resolve("datasqrl-cli.log");
    assertThat(cliLogFile).as("CLI log file should exist\n%s", logs).exists().isRegularFile();

    assertSoftly(
        softAssertions -> {
          try {
            var cliLogContent = Files.readString(cliLogFile);
            softAssertions
                .assertThat(cliLogContent)
                .as("CLI log file should contain content\n%s", logs)
                .isNotEmpty();

            log.info("CLI log file size: {} bytes", cliLogFile.toFile().length());
            log.debug(
                "CLI log content preview:\n{}",
                cliLogContent.length() > 500
                    ? cliLogContent.substring(0, 500) + "..."
                    : cliLogContent);

            // Validate that log files are not owned by root
            var cliLogOwner = Files.getOwner(cliLogFile);
            softAssertions
                .assertThat(cliLogOwner.getName())
                .as("CLI log file should not be owned by root\n%s", logs)
                .isNotEqualTo("root");

            // Check for service log files if they exist
            var redpandaLogFile = logsDir.resolve("redpanda.log");
            if (Files.exists(redpandaLogFile)) {
              var redpandaLogContent = Files.readString(redpandaLogFile);
              softAssertions
                  .assertThat(redpandaLogContent)
                  .as("Redpanda log file should contain content\n%s", logs)
                  .isNotEmpty();
              log.info("Redpanda log file size: {} bytes", redpandaLogFile.toFile().length());

              var redpandaLogOwner = Files.getOwner(redpandaLogFile);
              softAssertions
                  .assertThat(redpandaLogOwner.getName())
                  .as("Redpanda log file should not be owned by root\n%s", logs)
                  .isNotEqualTo("root");
            }

            var postgresLogFile = logsDir.resolve("postgres.log");
            if (Files.exists(postgresLogFile)) {
              var postgresLogContent = Files.readString(postgresLogFile);
              softAssertions
                  .assertThat(postgresLogContent)
                  .as("Postgres log file should contain content\n%s", logs)
                  .isNotEmpty();
              log.info("Postgres log file size: {} bytes", postgresLogFile.toFile().length());

              var postgresLogOwner = Files.getOwner(postgresLogFile);
              softAssertions
                  .assertThat(postgresLogOwner.getName())
                  .as("Postgres log file should not be owned by root\n%s", logs)
                  .isNotEqualTo("root");
            }

          } catch (Exception e) {
            softAssertions.fail("Failed to read log files: " + e.getMessage() + "\n" + logs);
          }
        });
  }

  protected void cleanupContainers() {
    if (serverContainer != null && serverContainer.isRunning()) {
      serverContainer.stop();
      serverContainer = null;
    }
    if (cmd != null && cmd.isRunning()) {
      cmd.stop();
      cmd = null;
    }
  }
}
