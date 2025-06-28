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
package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.graphql.config.CorsHandlerOptions;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.config.ServletConfig;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.datasqrl.graphql.server.operation.FunctionDefinition;
import com.datasqrl.graphql.server.operation.GraphQLQuery;
import com.datasqrl.graphql.server.operation.McpMethodType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphql.language.OperationDefinition;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.PoolOptions;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@ExtendWith(VertxExtension.class)
@Testcontainers
class McpApiValidationTest {

  private static final int MCP_SERVER_PORT = 8889;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private String getMcpInspectorImage() {
    // Get the MCP inspector version dynamically or use a fallback
    String mcpVersion = System.getProperty("mcp.inspector.version");
    if (mcpVersion == null || mcpVersion.trim().isEmpty()) {
      mcpVersion = "latest"; // fallback to latest when no specific version available
    }
    return "datasqrl/mcp-inspector:" + mcpVersion;
  }

  private Vertx vertx;
  private McpBridgeVerticle mcpBridge;
  private ServerConfig serverConfig;

  @SneakyThrows
  @BeforeEach
  void givenMcpServer_whenSetupTest_thenServerStartsSuccessfully(VertxTestContext testContext) {
    vertx = Vertx.vertx();

    // Create a GraphQL schema with MCP-enabled operations
    RootGraphqlModel root =
        RootGraphqlModel.builder()
            .schema(
                StringSchema.builder()
                    .schema(
                        """
                type Query {
                  getUserData(id: String!): User
                  getConfig: Config
                  searchUsers(query: String!): [User]
                }
                type User {
                  id: String
                  name: String
                  email: String
                }
                type Config {
                  version: String
                  environment: String
                }
                """)
                    .build())
            .operations(
                List.of(
                    // Tool operation - simple parameters
                    createApiOperation(
                        "getUserData",
                        "Get user data by ID",
                        Map.of("id", "string"),
                        List.of("id"),
                        McpMethodType.TOOL),
                    // Resource operation - no parameters
                    createApiOperation(
                        "getConfig",
                        "Get application configuration",
                        Map.of(),
                        List.of(),
                        McpMethodType.RESOURCE),
                    // Tool operation with complex parameters
                    createApiOperation(
                        "searchUsers",
                        "Search for users",
                        Map.of("query", "string"),
                        List.of("query"),
                        McpMethodType.TOOL)))
            .build();

    serverConfig = new ServerConfig();
    serverConfig.setPoolOptions(new PoolOptions());
    serverConfig.setServletConfig(new ServletConfig());
    serverConfig.setCorsHandlerOptions(new CorsHandlerOptions());

    HttpServerOptions httpServerOptions =
        new HttpServerOptions()
            .setPort(MCP_SERVER_PORT)
            .setHost("0.0.0.0"); // Bind to all interfaces for container access
    serverConfig.setHttpServerOptions(httpServerOptions);

    // Create a minimal HTTP server with just the MCP bridge
    var httpServer = vertx.createHttpServer(httpServerOptions);
    var router = io.vertx.ext.web.Router.router(vertx);

    // Add basic handlers
    router.route().handler(io.vertx.ext.web.handler.BodyHandler.create());

    // Create the MCP bridge verticle directly
    mcpBridge = new McpBridgeVerticle(router, serverConfig, root, Optional.empty(), null);

    vertx
        .deployVerticle(mcpBridge)
        .compose(
            deploymentId -> {
              httpServer.requestHandler(router);
              return httpServer.listen();
            })
        .onSuccess(
            server -> {
              System.out.println("MCP server started on port " + MCP_SERVER_PORT);
              testContext.completeNow();
            })
        .onFailure(testContext::failNow);
  }

  @SneakyThrows
  @AfterEach
  void teardown(VertxTestContext testContext) {
    if (vertx != null) {
      vertx.close().onComplete(ar -> testContext.completeNow()).onFailure(testContext::failNow);
    }
  }

  @Test
  void givenMcpServer_whenValidatedByMcpInspector_thenAllValidationsPass() throws Exception {
    // Get the host IP for container to access the MCP server
    String mcpUrl = String.format("http://host.testcontainers.internal:%d/mcp", MCP_SERVER_PORT);

    // Create MCP inspector container to validate the MCP server using pre-built image
    try (var mcpInspector =
        new GenericContainer<>(DockerImageName.parse(getMcpInspectorImage()))
            .withCommand(
                "sh",
                "-c",
                String.format(
                    """
            echo "Starting MCP validation for %s..." &&
            sleep 5 &&
            echo "Testing initialize..." &&
            npx @modelcontextprotocol/inspector --cli %s --method initialize &&
            echo "Testing tools/list..." &&
            npx @modelcontextprotocol/inspector --cli %s --method tools/list &&
            echo "Testing resources/list..." &&
            npx @modelcontextprotocol/inspector --cli %s --method resources/list &&
            echo "Testing ping..." &&
            npx @modelcontextprotocol/inspector --cli %s --method ping &&
            echo "All MCP validations completed successfully"
            """,
                    mcpUrl, mcpUrl, mcpUrl, mcpUrl, mcpUrl))
            .withLogConsumer(
                new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger("mcp-inspector")))
            .waitingFor(
                Wait.forLogMessage(".*All MCP validations completed successfully.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)))) {

      mcpInspector.start();

      // Get the validation results
      String logs = mcpInspector.getLogs();
      System.out.println("MCP Inspector Output:");
      System.out.println(logs);

      // Validate that essential MCP operations completed successfully
      assertMcpValidationResults(logs);
    }
  }

  @Test
  void givenMcpServer_whenValidatedWithDetailedOperations_thenProtocolComplianceIsValidated()
      throws Exception {
    String mcpUrl = String.format("http://host.testcontainers.internal:%d/mcp", MCP_SERVER_PORT);

    // Create a more detailed MCP validation container using pre-built image
    try (var validatorContainer =
        new GenericContainer<>(DockerImageName.parse(getMcpInspectorImage()))
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
            echo "=== Testing initialize ===" &&
            npx @modelcontextprotocol/inspector --cli %s --method initialize > /tmp/init_result.txt 2>&1 &&
            cat /tmp/init_result.txt &&
            echo "All detailed validations completed"
            """,
                    mcpUrl, mcpUrl, mcpUrl))
            .withLogConsumer(
                new Slf4jLogConsumer(org.slf4j.LoggerFactory.getLogger("mcp-detailed-validator")))
            .waitingFor(
                Wait.forLogMessage(".*All detailed validations completed.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)))) {

      validatorContainer.start();

      // Get the validation results
      String logs = validatorContainer.getLogs();
      System.out.println("Detailed MCP Validation Results:");
      System.out.println(logs);

      // Validate specific MCP protocol responses
      validateMcpProtocolCompliance(logs);
    }
  }

  private void assertMcpValidationResults(String logs) {
    // Check for successful completion message
    assertTrue(
        logs.contains("All MCP validations completed successfully"),
        "MCP validation should complete successfully");

    // Check that we don't have major errors that would prevent validation
    assertFalse(logs.contains("npm ERR!"), "Should not have npm installation errors");
    assertFalse(logs.contains("ECONNREFUSED"), "Should be able to connect to MCP server");

    // Check for successful method calls
    assertTrue(
        logs.contains("Testing initialize") || logs.contains("initialize"),
        "Should test initialize method");
    assertTrue(
        logs.contains("Testing tools/list") || logs.contains("tools"),
        "Should test tools/list method");
    assertTrue(
        logs.contains("Testing resources/list") || logs.contains("resources"),
        "Should test resources/list method");
  }

  private void validateMcpProtocolCompliance(String logs) throws Exception {
    // Look for JSON-RPC responses in the logs
    String[] lines = logs.split("\n");
    boolean foundValidToolsResponse = false;
    boolean foundValidResourcesResponse = false;
    boolean foundValidInitResponse = false;

    for (String line : lines) {
      String trimmedLine = line.trim();
      if (trimmedLine.startsWith("{") && trimmedLine.endsWith("}")) {
        try {
          JsonNode jsonResponse = objectMapper.readTree(trimmedLine);

          // Check if this is a valid JSON-RPC 2.0 response
          if (jsonResponse.has("jsonrpc") && "2.0".equals(jsonResponse.get("jsonrpc").asText())) {

            if (jsonResponse.has("result")) {
              JsonNode result = jsonResponse.get("result");

              // Check for tools response
              if (result.has("tools")) {
                foundValidToolsResponse = true;
                assertTrue(result.get("tools").isArray(), "Tools should be an array");

                if (result.get("tools").size() > 0) {
                  JsonNode firstTool = result.get("tools").get(0);
                  assertTrue(firstTool.has("name"), "Tool should have name");
                  assertTrue(firstTool.has("description"), "Tool should have description");
                  assertTrue(firstTool.has("inputSchema"), "Tool should have inputSchema");
                }
              }

              // Check for resources response
              if (result.has("resources")) {
                foundValidResourcesResponse = true;
                assertTrue(result.get("resources").isArray(), "Resources should be an array");

                if (result.get("resources").size() > 0) {
                  JsonNode firstResource = result.get("resources").get(0);
                  assertTrue(firstResource.has("uri"), "Resource should have uri");
                  assertTrue(firstResource.has("name"), "Resource should have name");
                }
              }

              // Check for initialize response
              if (result.has("protocolVersion")) {
                foundValidInitResponse = true;
                assertEquals(
                    "2024-11-05",
                    result.get("protocolVersion").asText(),
                    "Should use correct MCP protocol version");
                assertTrue(result.has("capabilities"), "Should have capabilities");
                assertTrue(result.has("serverInfo"), "Should have serverInfo");
              }
            }
          }
        } catch (Exception e) {
          // Not a JSON line or malformed JSON, continue
        }
      }
    }

    // At least one of the core MCP operations should have returned valid responses
    assertTrue(
        foundValidToolsResponse || foundValidResourcesResponse || foundValidInitResponse,
        "Should find at least one valid MCP protocol response");
  }

  private ApiOperation createApiOperation(
      String name,
      String description,
      Map<String, String> properties,
      List<String> required,
      McpMethodType mcpMethodType) {
    // Create function definition
    var propertiesMap =
        properties.entrySet().stream()
            .collect(
                java.util.stream.Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                      var argument = new FunctionDefinition.Argument();
                      argument.setType(entry.getValue());
                      argument.setDescription("Parameter " + entry.getKey());
                      return argument;
                    }));

    var parameters =
        FunctionDefinition.Parameters.builder()
            .type("object")
            .properties(propertiesMap)
            .required(required)
            .build();

    var function =
        FunctionDefinition.builder()
            .name(name)
            .description(description)
            .parameters(parameters)
            .build();

    // Create GraphQL query
    var graphQLQuery =
        new GraphQLQuery(
            String.format("query { %s }", name), name, OperationDefinition.Operation.QUERY);

    // Create API operation using the builder pattern
    return ApiOperation.getBuilder(function, graphQLQuery).mcpMethod(mcpMethodType).build();
  }
}
