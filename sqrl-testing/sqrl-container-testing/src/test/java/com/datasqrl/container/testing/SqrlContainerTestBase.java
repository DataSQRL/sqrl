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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.regex.Pattern;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class SqrlContainerTestBase {

  private static final Logger logger = LoggerFactory.getLogger(SqrlContainerTestBase.class);

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
    logger.info("Shared test resources initialized");
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
      logger.info("Shared test resources cleaned up");
    } catch (Exception e) {
      logger.warn("Error during shared resource cleanup", e);
    }
  }

  protected GenericContainer<?> createCmdContainer(String workingDir, String imageTag) {
    return new GenericContainer<>(DockerImageName.parse(SQRL_CMD_IMAGE + ":" + imageTag))
        .withNetwork(sharedNetwork)
        .withWorkingDirectory(BUILD_DIR)
        .withFileSystemBind(workingDir, BUILD_DIR)
        .withEnv("TZ", "UTC");
  }

  protected GenericContainer<?> createServerContainer(String workingDir, String imageTag) {
    var serverConfigPath =
        Paths.get(workingDir, "build", "deploy", "plan", "vertx.json").toString();
    var vertxConfigPath =
        Paths.get(workingDir, "build", "deploy", "plan", "vertx-config.json").toString();

    return new GenericContainer<>(DockerImageName.parse(SQRL_SERVER_IMAGE + ":" + imageTag))
        .withNetwork(sharedNetwork)
        .withExposedPorts(GRAPHQL_PORT)
        .withFileSystemBind(serverConfigPath, "/opt/sqrl/vertx.json")
        .withFileSystemBind(vertxConfigPath, "/opt/sqrl/vertx-config.json")
        .waitingFor(
            Wait.forLogMessage(".*GraphQL.*started.*", 1)
                .withStartupTimeout(Duration.ofSeconds(20)));
  }

  protected void compileSqrlScript(String scriptName, String workingDir, String imageTag) {
    cmdContainer = createCmdContainer(workingDir, imageTag).withCommand("compile", scriptName);

    cmdContainer.start();

    var exitCode = cmdContainer.getCurrentContainerInfo().getState().getExitCodeLong();
    if (exitCode != 0) {
      var logs = cmdContainer.getLogs();
      logger.error("SQRL compilation failed with exit code {}: {}", exitCode, logs);
      throw new RuntimeException("SQRL compilation failed with exit code " + exitCode);
    }

    logger.info("SQRL script {} compiled successfully", scriptName);
  }

  protected void startGraphQLServer(String workingDir, String imageTag) {
    serverContainer = createServerContainer(workingDir, imageTag);
    serverContainer.start();

    logger.info("GraphQL server started on port {}", serverContainer.getMappedPort(GRAPHQL_PORT));
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

  protected String getImageTag() {
    return System.getProperty("container.image.tag", "dev");
  }

  protected Path getTestResourcePath(String relativePath) {
    return Paths.get("src", "test", "resources", relativePath);
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
