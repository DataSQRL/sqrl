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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class SqrlContainerExtension
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  public static final String SQRL_CMD_IMAGE = "datasqrl/cmd";
  public static final String SQRL_SERVER_IMAGE = "datasqrl/sqrl-server";
  public static final String BUILD_DIR = "/build";
  public static final int HTTP_SERVER_PORT = 8888;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String testCaseName;

  @Getter private Network network;
  @Getter private CloseableHttpClient httpClient;
  @Getter private Path testDir;
  @Getter private GenericContainer<?> serverContainer;

  private List<GenericContainer<?>> commandContainers = new ArrayList<>();
  private long testStartTime;
  private String currentTestName;

  public SqrlContainerExtension(String testCaseName) {
    this.testCaseName = testCaseName;
  }

  @Override
  public void beforeAll(ExtensionContext context) {
    network = Network.newNetwork();
    var requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(10_000)
            .setSocketTimeout(30_000)
            .setConnectionRequestTimeout(5_000)
            .build();
    httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
    log.info("Shared test resources initialized");
  }

  @Override
  public void afterAll(ExtensionContext context) {
    try {
      if (httpClient != null) {
        httpClient.close();
      }
      if (network != null) {
        network.close();
      }
      log.info("Shared test resources cleaned up");
    } catch (Exception e) {
      log.warn("Error during shared resource cleanup", e);
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    currentTestName = context.getDisplayName();
    testStartTime = System.currentTimeMillis();
    log.info(">>> Starting: {}", currentTestName);
    testDir = itPath(testCaseName);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    cleanupContainers();
    var elapsed = System.currentTimeMillis() - testStartTime;
    log.info("<<< Finished: {} ({}ms)", currentTestName, elapsed);
  }

  public GenericContainer<?> createCmdContainer() {
    return createCmdContainer(true);
  }

  public GenericContainer<?> createCmdContainer(boolean debug) {
    assertThat(testDir).exists().isDirectory();

    var cmd =
        new GenericContainer<>(DockerImageName.parse(SQRL_CMD_IMAGE + ":" + getImageTag()))
            .withWorkingDirectory(BUILD_DIR)
            .withFileSystemBind(testDir.toString(), BUILD_DIR, BindMode.READ_WRITE)
            .withEnv("TZ", "America/Los_Angeles");
    if (debug) {
      cmd = cmd.withEnv("SQRL_DEBUG", "1");
    }

    commandContainers.add(cmd);
    return cmd;
  }

  @SuppressWarnings("resource")
  public GenericContainer<?> createServerContainer() {
    var deployPlanPath = testDir.resolve("build/deploy/plan");
    assertThat(deployPlanPath).exists().isDirectory();

    assertThat(serverContainer).as("serverContainer is already set.").isNull();
    serverContainer =
        new GenericContainer<>(DockerImageName.parse(SQRL_SERVER_IMAGE + ":" + getImageTag()))
            .withNetwork(network)
            .withExposedPorts(HTTP_SERVER_PORT)
            .withFileSystemBind(deployPlanPath.toString(), "/opt/sqrl/config", BindMode.READ_ONLY)
            .withEnv("SQRL_DEBUG", "1")
            .waitingFor(
                Wait.forLogMessage(".*HTTP server listening on port 8888.*", 1)
                    .withStartupTimeout(Duration.ofSeconds(30)));

    return serverContainer;
  }

  public void compileSqrlProject() {
    compileSqrlProject(null);
  }

  public void compileSqrlProject(@Nullable String packageFile) {
    var result = sqrlCmd("compile", packageFile != null ? packageFile : "package.json");
    validatePlan(result.logs());
    assertBuildNotOwnedByRoot(testDir, result.logs());
  }

  public void compileSqrlProject(
      @Nullable String packageFile, Consumer<GenericContainer<?>> customizer) {
    var result =
        sqrlCmd(customizer, true, "compile", packageFile != null ? packageFile : "package.json");
    validatePlan(result.logs());
    assertBuildNotOwnedByRoot(testDir, result.logs());
  }

  public ContainerResult sqrlCmd(String... command) {
    return sqrlCmd(true, command);
  }

  public ContainerResult sqrlCmd(boolean debug, String... command) {
    return sqrlCmd(c -> {}, debug, command);
  }

  public ContainerResult sqrlCmd(
      Consumer<GenericContainer<?>> customizer, boolean debug, String... command) {
    var cmd = createCmdContainer(debug).withCommand(command);
    customizer.accept(cmd);

    log.info("Docker run command to reproduce cmd:\n{}", getDockerRunCommand(cmd));

    commandContainers.add(cmd);
    cmd.start();

    // Wait for the container to finish running
    await().atMost(Duration.ofMinutes(5)).until(() -> !cmd.isRunning());

    var exitCode = cmd.getCurrentContainerInfo().getState().getExitCodeLong();
    var logs = cmd.getLogs();
    if (exitCode == null || exitCode != 0) {
      log.error("SQRL compilation failed with exit code {}\n{}", exitCode, logs);
      throw new ContainerError("SQRL compilation failed", exitCode, logs);
    }

    log.info("SQRL command {} completed successfully", Arrays.toString(command));

    return new ContainerResult(cmd, exitCode, logs);
  }

  private void validatePlan(String logs) {
    var planDir = testDir.resolve("build/deploy/plan");
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

  public void startGraphQLServer() {
    startGraphQLServer(c -> {});
  }

  public void startGraphQLServer(Consumer<GenericContainer<?>> containerCustomizer) {
    serverContainer = createServerContainer();

    containerCustomizer.accept(serverContainer);

    log.info("Docker run command to reproduce server:\n{}", getDockerRunCommand(serverContainer));

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

  public String getBaseUrl() {
    if (serverContainer == null || !serverContainer.isRunning()) {
      throw new IllegalStateException("Server container is not running");
    }
    return "http://localhost:" + serverContainer.getMappedPort(HTTP_SERVER_PORT);
  }

  public String getGraphQLEndpoint() {
    return getBaseUrl() + "/v1/graphql";
  }

  public String getMetricsEndpoint() {
    return getBaseUrl() + "/metrics";
  }

  public String getHealthEndpoint() {
    return getBaseUrl() + "/health";
  }

  private String getImageTag() {
    return System.getProperty("docker.image.tag", "local");
  }

  public String getDockerRunCommand(GenericContainer<?> container) {
    var sb = new StringBuilder();
    sb.append("docker run -it --rm");

    var exposedPorts = container.getExposedPorts();
    if (exposedPorts != null && !exposedPorts.isEmpty()) {
      for (var port : exposedPorts) {
        sb.append(" -p ").append(port).append(":").append(port);
      }
    }

    var binds = container.getBinds();
    if (binds != null && !binds.isEmpty()) {
      for (var bind : binds) {
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

    var env = container.getEnvMap();
    if (env != null && !env.isEmpty()) {
      for (var entry : env.entrySet()) {
        sb.append(" -e ").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }

    var containerNetwork = container.getNetwork();
    if (containerNetwork != null) {
      sb.append(" --network ").append(containerNetwork.getId());
    }

    var dockerImageName = container.getDockerImageName();
    sb.append(" ").append(dockerImageName);

    var command = container.getCommandParts();
    if (command != null) {
      for (String part : command) {
        sb.append(" ").append(part);
      }
    }

    return sb.toString();
  }

  @SneakyThrows
  public static Path itPath(String relativePath) {
    var directLocalPath = Paths.get("src/test/resources", relativePath).toAbsolutePath();
    if (Files.exists(directLocalPath) && Files.isDirectory(directLocalPath)) {
      return directLocalPath.toRealPath();
    }

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

  public static void assertBuildNotOwnedByRoot(Path testDir, String logs) {
    var buildPath = testDir.resolve("build");
    assertThat(buildPath).exists();

    assertOwner(buildPath, logs);
  }

  public static void assertOwner(Path path, String logs) {
    try {
      var owner = Files.getOwner(path);
      assertThat(owner.getName())
          .as("Build directory should not be owned by root user: %s\n%s", path, logs)
          .isNotEqualTo("root");
      log.debug("Build directory {} is owned by: {}", path, owner.getName());
    } catch (IOException e) {
      fail("Failed to check build directory ownership: " + e.getMessage(), e);
    }
  }

  public void validateBasicGraphQLResponse(HttpResponse response) throws Exception {
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    var responseBody = EntityUtils.toString(response.getEntity());
    var jsonResponse = OBJECT_MAPPER.readTree(responseBody);

    assertThat(jsonResponse.has("data")).isTrue();
    assertThat(jsonResponse.get("data").has("__typename")).isTrue();
    assertThat(jsonResponse.get("data").get("__typename").asText()).isEqualTo("Query");
  }

  public void compileAndStartServer() {
    compileAndStartServer(null);
  }

  public void compileAndStartServer(@Nullable String packageFile) {
    compileSqrlProject(packageFile);
    startGraphQLServer();
  }

  public CloseableHttpResponse executeGraphQLQuery(String query) throws Exception {
    return executeGraphQLQuery(query, null);
  }

  public CloseableHttpResponse executeGraphQLQuery(String query, String jwtToken) throws Exception {
    var request = new HttpPost(getGraphQLEndpoint());
    request.setEntity(new StringEntity(query, ContentType.APPLICATION_JSON));

    if (jwtToken != null) {
      request.setHeader("Authorization", "Bearer " + jwtToken);
    }

    return httpClient.execute(request);
  }

  public void assertLogFiles(String logs) {
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

            var cliLogOwner = Files.getOwner(cliLogFile);
            softAssertions
                .assertThat(cliLogOwner.getName())
                .as("CLI log file should not be owned by root\n%s", logs)
                .isNotEqualTo("root");

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

          } catch (IOException e) {
            softAssertions.fail("Failed to read log files: " + e.getMessage() + "\n" + logs);
          }
        });
  }

  public void cleanupContainers() {
    if (serverContainer != null) {
      serverContainer.stop();
      serverContainer = null;
    }
    commandContainers.forEach(Startable::close);
    commandContainers = new ArrayList<>();
  }
}
