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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs a pipeline with nested mutation inputs and verifies that MCP tools accept nested objects and
 * lists of objects, and that the nested data can be read back through MCP query tools.
 */
@Slf4j
class McpNestedInputContainerIT {

  @RegisterExtension
  static SqrlContainerExtension sqrl = new SqrlContainerExtension("mcp-nested-input");

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  @SneakyThrows
  void givenNestedMutationInputs_whenCalledThroughMcp_thenNestedDataRoundTrips() {
    var run =
        sqrl.createCmdContainer()
            .withCommand("run")
            .withExposedPorts(SqrlContainerExtension.HTTP_SERVER_PORT)
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(SqrlContainerExtension.HTTP_SERVER_PORT)
                    // 200 with check details once the Kafka health check is registered
                    .forStatusCode(200)
                    .forStatusCode(204)
                    .withStartupTimeout(Duration.ofMinutes(2)));
    run.start();

    var client = createMcpClient(run);
    try {
      client.initialize();

      var tools =
          client.listTools().tools().stream()
              .collect(Collectors.toMap(McpSchema.Tool::name, Function.identity()));
      assertThat(tools).containsKeys("AddOrders", "AddReadings", "GetCustomerOrders");
      var orderSchema = mapper.valueToTree(tools.get("AddOrders").inputSchema());
      assertThat(orderSchema.at("/properties/shipping/properties/address/required"))
          .extracting(JsonNode::asText)
          .containsExactlyInAnyOrder("street", "city");
      assertThat(orderSchema.at("/properties/entries/items/type").asText()).isEqualTo("object");
      var readingSchema = mapper.valueToTree(tools.get("AddReadings").inputSchema());
      assertThat(readingSchema.at("/properties/event/items/type").asText()).isEqualTo("object");

      var address = new HashMap<String, Object>();
      address.put("street", "Main St 1");
      address.put("city", "Berlin");
      address.put("zip", null);
      var order =
          Map.<String, Object>of(
              "id",
              1,
              "customerid",
              42,
              "shipping",
              Map.of("name", "Ada", "address", address),
              "entries",
              List.of(
                  Map.of("productid", 7, "quantity", 2, "unit_price", 9.99),
                  Map.of("productid", 8, "quantity", 1, "unit_price", 5.0, "discount", 0.5)));
      var added = callTool(client, "AddOrders", order);
      assertThat(added.at("/shipping/address/city").asText()).isEqualTo("Berlin");

      var readings =
          Map.<String, Object>of(
              "event",
              List.of(
                  Map.of("sensorid", 1, "temperature", 21.5),
                  Map.of("sensorid", 1, "temperature", 19.0)));
      assertThat(callTool(client, "AddReadings", readings)).hasSize(2);

      var invalidOrder = new HashMap<>(order);
      invalidOrder.put("shipping", Map.of("name", "Ada", "address", Map.of("street", "Main St 1")));
      var invalidResult = client.callTool(toolRequest("AddOrders", invalidOrder));
      assertThat(invalidResult.isError()).isTrue();
      assertThat(text(invalidResult)).startsWith("Validation error").contains("city");

      var orders =
          awaitResults(
              client,
              "GetCustomerOrders",
              Map.of("customerid", 42),
              results -> results.size() == 1);
      var storedOrder = orders.get(0);
      assertThat(storedOrder.at("/shipping/name").asText()).isEqualTo("Ada");
      assertThat(storedOrder.at("/shipping/address/city").asText()).isEqualTo("Berlin");
      assertThat(storedOrder.at("/shipping/address/zip").isNull()).isTrue();
      assertThat(storedOrder.get("entries")).hasSize(2);
      assertThat(storedOrder.at("/entries/0/productid").asLong()).isEqualTo(7L);
      assertThat(storedOrder.at("/entries/1/discount").asDouble()).isEqualTo(0.5);

      var storedReadings =
          awaitResults(
              client, "GetSensorReadings", Map.of("sensorid", 1), results -> results.size() == 2);
      assertThat(storedReadings.at("/0/temperature").asDouble()).isEqualTo(19.0);
      assertThat(storedReadings.at("/1/temperature").asDouble()).isEqualTo(21.5);
    } catch (Throwable e) {
      log.error("Run container logs:\n{}", run.getLogs());
      throw e;
    } finally {
      client.close();
    }
  }

  private static McpSyncClient createMcpClient(GenericContainer<?> container) {
    var baseUrl =
        "http://localhost:" + container.getMappedPort(SqrlContainerExtension.HTTP_SERVER_PORT);
    var transport = HttpClientStreamableHttpTransport.builder(baseUrl).endpoint("/v1/mcp").build();
    return McpClient.sync(transport).requestTimeout(Duration.ofSeconds(30)).build();
  }

  private JsonNode callTool(McpSyncClient client, String name, Map<String, Object> arguments) {
    var result = client.callTool(toolRequest(name, arguments));
    assertThat(result.isError()).as("Tool %s returned: %s", name, text(result)).isFalse();
    return readJson(text(result));
  }

  private JsonNode awaitResults(
      McpSyncClient client,
      String name,
      Map<String, Object> arguments,
      Predicate<JsonNode> condition) {
    return await()
        .atMost(Duration.ofMinutes(2))
        .pollInterval(Duration.ofSeconds(2))
        .until(() -> callTool(client, name, arguments), condition);
  }

  private static McpSchema.CallToolRequest toolRequest(String name, Map<String, Object> arguments) {
    return McpSchema.CallToolRequest.builder(name).arguments(arguments).build();
  }

  private static String text(McpSchema.CallToolResult result) {
    return ((McpSchema.TextContent) result.content().get(0)).text();
  }

  @SneakyThrows
  private JsonNode readJson(String json) {
    return mapper.readTree(json);
  }
}
