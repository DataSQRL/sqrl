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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class SqrlContainerTestBase {

  private static final Logger log = LoggerFactory.getLogger(SqrlContainerTestBase.class);

  protected static final String SQRL_CMD_IMAGE = "datasqrl/cmd";
  protected static final String SQRL_SERVER_IMAGE = "datasqrl/sqrl-server";
  protected static final String BUILD_DIR = "/build";
  protected static final int GRAPHQL_PORT = 8888;

  protected static Network sharedNetwork;
  protected static CloseableHttpClient sharedHttpClient;
  protected static final ObjectMapper objectMapper = new ObjectMapper();

  protected GenericContainer<?> cmdContainer;
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
    assertThat(workingDir).exists().isDirectory();

    return new GenericContainer<>(DockerImageName.parse(SQRL_CMD_IMAGE + ":" + getImageTag()))
        .withNetwork(sharedNetwork)
        .withWorkingDirectory(BUILD_DIR)
        .withFileSystemBind(workingDir.toString(), BUILD_DIR, BindMode.READ_WRITE)
        .withEnv("TZ", "UTC");
  }

  protected GenericContainer<?> createServerContainer(Path workingDir) {
    var deployPlanPath = workingDir.resolve("build/deploy/plan");
    assertThat(deployPlanPath).exists().isDirectory();

    return new GenericContainer<>(DockerImageName.parse(SQRL_SERVER_IMAGE + ":" + getImageTag()))
        .withNetwork(sharedNetwork)
        .withExposedPorts(GRAPHQL_PORT)
        .withFileSystemBind(deployPlanPath.toString(), "/opt/sqrl/config", BindMode.READ_ONLY)
        .waitingFor(
            Wait.forLogMessage(".*GraphQLServer.*started.*", 1)
                .withStartupTimeout(Duration.ofSeconds(20)));
  }

  protected void compileSqrlScript(String scriptName, Path workingDir) {
    cmdContainer = createCmdContainer(workingDir).withCommand("compile", scriptName);

    cmdContainer.start();

    var exitCode = cmdContainer.getCurrentContainerInfo().getState().getExitCodeLong();
    if (exitCode != 0) {
      var logs = cmdContainer.getLogs();
      log.error("SQRL compilation failed with exit code {}: {}", exitCode, logs);
      log.error("Docker run command to reproduce:");
      log.error(
          getDockerRunCommand(
              workingDir, SQRL_CMD_IMAGE, getImageTag(), false, "compile", scriptName));
      throw new RuntimeException("SQRL compilation failed with exit code " + exitCode);
    }

    log.info("SQRL script {} compiled successfully \n{}", scriptName, cmdContainer.getLogs());
  }

  protected void startGraphQLServer(Path workingDir) {
    serverContainer = createServerContainer(workingDir);

    try {
      serverContainer.start();
      log.info("GraphQL server started on port {}", serverContainer.getMappedPort(GRAPHQL_PORT));
    } catch (Exception e) {
      log.error("Failed to start GraphQL server container:");
      log.error("Container image: {}", SQRL_SERVER_IMAGE + ":" + getImageTag());
      log.error("Docker run command to reproduce:");
      log.error(getDockerRunCommand(workingDir, SQRL_SERVER_IMAGE, getImageTag(), true));
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

  protected String getGraphQLEndpoint() {
    if (serverContainer == null || !serverContainer.isRunning()) {
      throw new IllegalStateException("Server container is not running");
    }
    return "http://localhost:" + serverContainer.getMappedPort(GRAPHQL_PORT) + "/graphql";
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
    return System.getProperty("container.image.tag", "local");
  }

  private String getDockerRunCommand(
      Path workingDir, String imageName, String imageTag, boolean isServer, String... commands) {
    var sb = new StringBuilder();
    sb.append("docker run -it --rm");

    if (isServer) {
      var deployPlanPath = workingDir.resolve("build/deploy/plan").toString();
      sb.append(" -p ").append(GRAPHQL_PORT).append(":").append(GRAPHQL_PORT);
      sb.append(" -v \"").append(deployPlanPath).append(":/opt/sqrl/config\"");
    } else {
      sb.append(" -v \"").append(workingDir).append(":").append(BUILD_DIR).append("\"");
      sb.append(" -w ").append(BUILD_DIR);
      sb.append(" -e TZ=UTC");
    }

    sb.append(" ").append(imageName).append(":").append(imageTag);

    for (String command : commands) {
      sb.append(" ").append(command);
    }

    return sb.toString();
  }

  @SneakyThrows
  protected Path itPath(String relativePath) {
    var path =
        Paths.get("../sqrl-integration-tests/src/test/resources/usecases", relativePath)
            .toAbsolutePath();
    assertThat(path).exists().isDirectory();
    return path.toRealPath();
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

  protected void cleanupContainers() {
    if (serverContainer != null && serverContainer.isRunning()) {
      serverContainer.stop();
      serverContainer = null;
    }
    if (cmdContainer != null && cmdContainer.isRunning()) {
      cmdContainer.stop();
      cmdContainer = null;
    }
  }
}
