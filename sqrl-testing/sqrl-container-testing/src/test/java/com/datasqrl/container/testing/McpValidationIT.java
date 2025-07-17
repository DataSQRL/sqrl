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

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@Slf4j
class McpValidationIT extends SqrlContainerTestBase {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  protected String getTestCaseName() {
    return "udf";
  }

  private String getMcpInspectorImage() {
    var mcpVersion = System.getProperty("mcp.inspector.version");
    if (mcpVersion == null || mcpVersion.trim().isEmpty()) {
      mcpVersion = "latest";
    }
    return "datasqrl/mcp-inspector:" + mcpVersion;
  }

  private GenericContainer<?> createMcpInspectorContainer(String imageName) {
    var container = new GenericContainer<>(DockerImageName.parse(imageName));

    // Always pull if tag is "latest"
    if (imageName.endsWith(":latest")) {
      container.withImagePullPolicy(PullPolicy.alwaysPull());
    }

    return container;
  }

  @Test
  void givenUdfTestCase_whenMcpServerStarted_thenMcpInspectorValidatesSuccessfully()
      throws Exception {
    var serverAlias = "sqrl-server";
    var mcpUrl = setupMcpServer(serverAlias);

    var mcpInspector = createMcpInspectorForValidation(mcpUrl);

    runMcpValidation(mcpInspector, this::assertMcpValidationResults);
  }

  private String setupMcpServer(String serverAlias) throws Exception {
    compileSqrlScript("myudf.sqrl", testDir);

    serverContainer = createServerContainer(testDir);
    serverContainer.withNetworkAliases(serverAlias);
    serverContainer.start();
    log.info("HTTP server started on port {}", serverContainer.getMappedPort(HTTP_SERVER_PORT));

    var mcpUrl = String.format("http://%s:8888/mcp", serverAlias);
    log.info("Testing MCP endpoint: {}", mcpUrl);
    return mcpUrl;
  }

  private GenericContainer<?> createMcpInspectorForValidation(String mcpUrl) {
    return createMcpInspectorContainer(getMcpInspectorImage())
        .withNetwork(sharedNetwork)
        .withCommand(
            "sh",
            "-c",
            String.format(
                """
        echo "Starting MCP validation for %s..." &&
        sleep 5 &&
        echo "Testing tools/list..." &&
        npx @modelcontextprotocol/inspector --cli %s --method tools/list &&
        echo "Testing resources/list..." &&
        npx @modelcontextprotocol/inspector --cli %s --method resources/list &&
        echo "All MCP validations completed successfully"
        """,
                mcpUrl, mcpUrl, mcpUrl))
        .withLogConsumer(new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger("mcp-inspector")))
        .waitingFor(
            Wait.forLogMessage(".*All MCP validations completed successfully.*", 1)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private GenericContainer<?> createMcpInspectorForDetailedValidation(String mcpUrl) {
    return createMcpInspectorContainer(getMcpInspectorImage())
        .withNetwork(sharedNetwork)
        .withCommand(
            "sh",
            "-c",
            String.format(
                """
        echo "Starting detailed MCP validation..." &&
        sleep 5 &&
        echo "=== Testing tools/list ===" &&
        npx @modelcontextprotocol/inspector --cli %s --method tools/list > /tmp/tools_result.txt 2>&1 &&
        cat /tmp/tools_result.txt &&
        echo "=== Testing resources/list ===" &&
        npx @modelcontextprotocol/inspector --cli %s --method resources/list > /tmp/resources_result.txt 2>&1 &&
        cat /tmp/resources_result.txt &&
        echo "All detailed validations completed"
        """,
                mcpUrl, mcpUrl))
        .withLogConsumer(
            new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger("mcp-detailed-validator")))
        .waitingFor(
            Wait.forLogMessage(".*All detailed validations completed.*", 1)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  private void runMcpValidation(
      GenericContainer<?> inspector, java.util.function.Consumer<String> validator)
      throws Exception {
    try {
      inspector.start();
      var logs = inspector.getLogs();
      log.info("MCP Inspector Output:");
      log.info(logs);
      validator.accept(logs);
    } catch (Exception e) {
      logContainerError(inspector, e);
      throw e;
    } finally {
      stopContainer(inspector);
    }
  }

  private void logContainerError(GenericContainer<?> container, Exception e) {
    try {
      log.error("=== CONTAINER LOGS (due to test failure) ===");
      log.error(container.getLogs());
      log.error("=== END CONTAINER LOGS ===");
    } catch (Exception logException) {
      log.error("Failed to retrieve container logs: {}", logException.getMessage());
    }
  }

  private void stopContainer(GenericContainer<?> container) {
    try {
      container.stop();
    } catch (Exception stopException) {
      log.error("Failed to stop container: {}", stopException.getMessage());
    }
  }

  @Test
  void givenUdfTestCase_whenMcpServerStarted_thenDetailedProtocolValidationPasses()
      throws Exception {
    var serverAlias = "sqrl-server";
    var mcpUrl = setupMcpServer(serverAlias);

    var validatorContainer = createMcpInspectorForDetailedValidation(mcpUrl);

    runMcpValidation(validatorContainer, this::validateMcpProtocolCompliance);
  }

  private void assertMcpValidationResults(String logs) {
    assertThat(logs)
        .as("MCP validation should complete successfully")
        .contains("All MCP validations completed successfully");

    assertThat(logs).as("Should not have npm installation errors").doesNotContain("npm ERR!");
    assertThat(logs).as("Should be able to connect to MCP server").doesNotContain("ECONNREFUSED");

    assertThat(logs.contains("Testing tools/list") || logs.contains("tools"))
        .as("Should test tools/list method")
        .isTrue();
    assertThat(logs.contains("Testing resources/list") || logs.contains("resources"))
        .as("Should test resources/list method")
        .isTrue();
  }

  @SneakyThrows
  private void validateMcpProtocolCompliance(String logs) {
    var foundValidToolsResponse = false;
    var foundValidResourcesResponse = false;

    // Look for multi-line JSON blocks
    var jsonStart = logs.indexOf("{");
    while (jsonStart != -1) {
      var braceCount = 0;
      var jsonEnd = jsonStart;

      for (int i = jsonStart; i < logs.length(); i++) {
        var ch = logs.charAt(i);
        if (ch == '{') braceCount++;
        else if (ch == '}') braceCount--;

        if (braceCount == 0) {
          jsonEnd = i;
          break;
        }
      }

      if (braceCount == 0) {
        var jsonText = logs.substring(jsonStart, jsonEnd + 1);
        try {
          var jsonResponse = objectMapper.readTree(jsonText);

          // Check for direct JSON responses (not wrapped in JSON-RPC)
          if (jsonResponse.has("tools")) {
            foundValidToolsResponse = true;
            assertThat(jsonResponse.get("tools").isArray()).as("Tools should be an array").isTrue();

            if (jsonResponse.get("tools").size() > 0) {
              var firstTool = jsonResponse.get("tools").get(0);
              assertThat(firstTool.has("name")).as("Tool should have name").isTrue();
              assertThat(firstTool.has("description")).as("Tool should have description").isTrue();
              assertThat(firstTool.has("inputSchema")).as("Tool should have inputSchema").isTrue();
            }
          }

          if (jsonResponse.has("resources")) {
            foundValidResourcesResponse = true;
            assertThat(jsonResponse.get("resources").isArray())
                .as("Resources should be an array")
                .isTrue();
          }

          // Also check JSON-RPC 2.0 wrapped responses
          if (jsonResponse.has("jsonrpc") && "2.0".equals(jsonResponse.get("jsonrpc").asText())) {
            if (jsonResponse.has("result")) {
              var result = jsonResponse.get("result");

              if (result.has("tools")) {
                foundValidToolsResponse = true;
                assertThat(result.get("tools").isArray()).as("Tools should be an array").isTrue();
              }

              if (result.has("resources")) {
                foundValidResourcesResponse = true;
                assertThat(result.get("resources").isArray())
                    .as("Resources should be an array")
                    .isTrue();
              }
            }
          }
        } catch (Exception e) {
          // Not valid JSON, continue
        }
      }

      jsonStart = logs.indexOf("{", jsonEnd + 1);
    }

    assertThat(foundValidToolsResponse || foundValidResourcesResponse)
        .as("Should find at least one valid MCP protocol response (tools or resources)")
        .isTrue();
  }
}
