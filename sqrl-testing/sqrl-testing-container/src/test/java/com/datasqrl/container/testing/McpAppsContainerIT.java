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
import static org.awaitility.Awaitility.await;

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs a pipeline with MCP apps and verifies that the MCP endpoint links the tools to the apps and
 * serves the apps as resources, as MCP hosts with the MCP Apps extension read them.
 */
@Slf4j
class McpAppsContainerIT {

  @RegisterExtension static SqrlContainerExtension sqrl = new SqrlContainerExtension("mcp-apps");

  @Test
  void givenMcpApps_whenServedThroughMcp_thenToolsLinkAppsThatHostsCanRead() {
    var run =
        sqrl.createCmdContainer()
            .withCommand("run")
            .withExposedPorts(SqrlContainerExtension.HTTP_SERVER_PORT)
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(SqrlContainerExtension.HTTP_SERVER_PORT)
                    .forStatusCode(200)
                    .forStatusCode(204)
                    .withStartupTimeout(Duration.ofMinutes(2)));
    run.start();

    try (var client = createMcpClient(run)) {
      var capabilities = client.initialize().capabilities();
      assertThat(capabilities.extensions())
          .containsEntry(
              "io.modelcontextprotocol/ui",
              Map.of("mimeTypes", List.of("text/html;profile=mcp-app")));

      var tools =
          client.listTools().tools().stream()
              .collect(Collectors.toMap(McpSchema.Tool::name, Function.identity()));
      assertThat(tools.get("GetSales").meta())
          .isEqualTo(
              Map.of(
                  "ui",
                  Map.of(
                      "resourceUri",
                      "ui://apps/SalesChart",
                      "visibility",
                      List.of("model", "app"))));
      assertThat(tools.get("GetSalesByRegion").meta())
          .isEqualTo(
              Map.of(
                  "ui",
                  Map.of(
                      "resourceUri",
                      "ui://apps/getSalesByRegion",
                      "visibility",
                      List.of("model", "app"))));
      assertThat(tools.get("GetRegionSales").meta())
          .isEqualTo(Map.of("ui", Map.of("visibility", List.of("app"))));

      assertThat(client.listResources().resources())
          .extracting(McpSchema.Resource::uri)
          .containsExactlyInAnyOrder("ui://apps/SalesChart", "ui://apps/getSalesByRegion");
      var contents =
          client.readResource(new McpSchema.ReadResourceRequest("ui://apps/SalesChart")).contents();
      assertThat(contents)
          .singleElement()
          .isInstanceOfSatisfying(
              McpSchema.TextResourceContents.class,
              app -> {
                assertThat(app.mimeType()).isEqualTo("text/html;profile=mcp-app");
                assertThat(app.text()).contains("<title>Sales Chart</title>");
              });

      // An app calls its app-only tool through the host as an ordinary tool call
      var regionSales =
          await()
              .atMost(Duration.ofMinutes(2))
              .pollInterval(Duration.ofSeconds(2))
              .until(
                  () -> text(client.callTool(toolRequest("GetRegionSales", "north"))),
                  result -> result.contains("widget"));
      assertThat(regionSales).doesNotContain("gadget");
    }
  }

  private static McpSyncClient createMcpClient(GenericContainer<?> container) {
    var baseUrl =
        "http://localhost:" + container.getMappedPort(SqrlContainerExtension.HTTP_SERVER_PORT);
    var transport = HttpClientStreamableHttpTransport.builder(baseUrl).endpoint("/v1/mcp").build();
    return McpClient.sync(transport).requestTimeout(Duration.ofSeconds(30)).build();
  }

  private static McpSchema.CallToolRequest toolRequest(String name, String region) {
    return McpSchema.CallToolRequest.builder(name).arguments(Map.of("region", region)).build();
  }

  private static String text(McpSchema.CallToolResult result) {
    return ((McpSchema.TextContent) result.content().get(0)).text();
  }
}
