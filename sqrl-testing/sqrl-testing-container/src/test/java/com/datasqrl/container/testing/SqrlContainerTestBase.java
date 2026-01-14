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
import javax.annotation.Nullable;
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
  protected static final String JACOCO_OUT_DIR = "/jacoco";
  protected static final String JACOCO_AGENT_PATH = "/jacoco-agent.jar";
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

    container = configureJacocoCoverage(container, "cmd");

    if (debug) {
      container = container.withEnv("SQRL_DEBUG", "1");
    }

    return container;
  }

  @SuppressWarnings("resource")
  protected GenericContainer<?> createServerContainer(Path workingDir) {
    var deployPlanPath = workingDir.resolve("build/deploy/plan");
    assertThat(deployPlanPath).exists().isDirectory();

    var container =
        new GenericContainer<>(DockerImageName.parse(SQRL_SERVER_IMAGE + ":" + getImageTag()))
            .withNetwork(sharedNetwork)
            .withExposedPorts(HTTP_SERVER_PORT)
            .withFileSystemBind(deployPlanPath.toString(), "/opt/sqrl/config", BindMode.READ_ONLY)
            .withEnv("SQRL_DEBUG", "1")
            .waitingFor(
                Wait.forLogMessage(".*HTTP server listening on port 8888.*", 1)
                    .withStartupTimeout(Duration.ofSeconds(30)));

    container = configureJacocoCoverage(container, "server");

    return container;
  }

  protected void compileSqrlProject(Path workingDir) {
    compileSqrlProject(workingDir, null);
  }

  protected void compileSqrlProject(Path workingDir, @Nullable String packageFile) {
    sqrlCmd(workingDir, "compile", packageFile != null ? packageFile : "package.json");
  }

  protected ContainerResult sqrlCmd(Path workingDir, String... command) {
    return sqrlCmd(workingDir, true, command);
  }

  protected ContainerResult sqrlCmd(Path workingDir, boolean debug, String... command) {
    cmd = createCmdContainer(workingDir, debug).withCommand(command);

    log.info("Docker run command to reproduce:");
    log.info(getDockerRunCommand(cmd, workingDir));

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
    serverContainer = createServerContainer(workingDir);

    containerCustomizer.accept(serverContainer);

    log.info("Docker run command to reproduce:");
    log.info(getDockerRunCommand(serverContainer, workingDir));

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
    return getBaseUrl() + "/v1/graphql";
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

  @SneakyThrows
  private GenericContainer<?> configureJacocoCoverage(GenericContainer<?> container, String component) {
    if (!Boolean.parseBoolean(System.getProperty("sqrl.container.coverage", "false"))) {
      return container;
    }

    var jacocoAgentJar = findJacocoAgentJar();
    if (jacocoAgentJar == null) {
      log.warn("Container coverage enabled but JaCoCo agent jar not found in ~/.m2; skipping");
      return container;
    }

    var jacocoOutDir = Paths.get("target", "jacoco").toAbsolutePath();
    var hostDestFile = jacocoOutDir.resolve("jacoco-container.exec");
    Files.createDirectories(jacocoOutDir);

    var containerDestFile = JACOCO_OUT_DIR + "/jacoco-container.exec";
    var agentArg =
        "-javaagent:"
            + JACOCO_AGENT_PATH
            + "=destfile="
            + containerDestFile
            + ",append=true,excludes=org/**";

    var toolOptions = agentArg;

    log.info(
        "Enabling container JaCoCo for {}: agent={} destfile={}",
        component,
        jacocoAgentJar,
        hostDestFile);

    return container
        .withFileSystemBind(jacocoOutDir.toString(), JACOCO_OUT_DIR, BindMode.READ_WRITE)
        .withFileSystemBind(jacocoAgentJar.toString(), JACOCO_AGENT_PATH, BindMode.READ_ONLY)
        .withEnv("JAVA_TOOL_OPTIONS", toolOptions);
  }

  @Nullable
  private Path findJacocoAgentJar() {
    var m2 = Paths.get(System.getProperty("user.home"), ".m2", "repository");
    var jacocoDir = m2.resolve(Paths.get("org", "jacoco", "org.jacoco.agent"));
    if (!Files.exists(jacocoDir)) {
      return null;
    }

    try (var stream = Files.list(jacocoDir)) {
      var versions =
          stream
              .filter(Files::isDirectory)
              .map(p -> p.getFileName().toString())
              .sorted()
              .toList();
      for (int i = versions.size() - 1; i >= 0; i--) {
        var version = versions.get(i);
        var candidate =
            jacocoDir.resolve(
                Paths.get(version, "org.jacoco.agent-" + version + "-runtime.jar"));
        if (Files.exists(candidate)) {
          return candidate;
        }
      }
      return null;
    } catch (Exception e) {
      log.warn("Failed to locate JaCoCo agent jar under {}", jacocoDir, e);
      return null;
    }
  }

  protected String getDockerRunCommand(GenericContainer<?> container, Path workingDir) {
    var sb = new StringBuilder();
    sb.append("docker run -it --rm");

    // Extract ports
    var exposedPorts = container.getExposedPorts();
    if (exposedPorts != null && !exposedPorts.isEmpty()) {
      for (var port : exposedPorts) {
        sb.append(" -p ").append(port).append(":").append(port);
      }
    }

    // Extract volume binds
    var binds = container.getBinds();
    if (binds != null && !binds.isEmpty()) {
      for (var bind : binds) {
        // Parse the bind string which should be in format "hostPath:containerPath" or
        // "hostPath:containerPath:mode"
        var bindString = bind.getPath();
        var parts = bindString.split(":");
        if (parts.length >= 2) {
          sb.append(" -v \"").append(parts[0]).append(":").append(parts[1]);
          if (parts.length > 2) {
            sb.append(":").append(parts[2]);
          }
          sb.append("\"");
        } else {
          sb.append(" -v \"").append(bindString).append("\"");
        }
      }
    }

    // Extract environment variables
    var env = container.getEnvMap();
    if (env != null && !env.isEmpty()) {
      for (var entry : env.entrySet()) {
        sb.append(" -e ").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }

    // Extract network
    var network = container.getNetwork();
    if (network != null) {
      sb.append(" --network ").append(network.getId());
    }

    // Extract image name
    var dockerImageName = container.getDockerImageName();
    sb.append(" ").append(dockerImageName);

    // Extract command
    var command = container.getCommandParts();
    if (command != null) {
      for (String part : command) {
        sb.append(" ").append(part);
      }
    }

    return sb.toString();
  }

  @SneakyThrows
  protected static Path itPath(String relativePath) {
    var localPath = Paths.get("src/test/resources/usecases", relativePath).toAbsolutePath();
    if (Files.exists(localPath) && Files.isDirectory(localPath)) {
      return localPath.toRealPath();
    }

    var integrationPath =
        Paths.get("../sqrl-testing-integration/src/test/resources/usecases", relativePath)
            .toAbsolutePath();
    assertThat(integrationPath).exists().isDirectory();
    return integrationPath.toRealPath();
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

  protected void compileAndStartServer(Path testDir) {
    compileAndStartServer(testDir, null);
  }

  protected void compileAndStartServer(Path testDir, @Nullable String packageFile) {
    compileSqrlProject(testDir, packageFile);
    startGraphQLServer(testDir);
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
