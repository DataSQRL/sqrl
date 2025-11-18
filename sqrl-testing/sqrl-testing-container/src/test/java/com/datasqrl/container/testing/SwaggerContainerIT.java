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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

public class SwaggerContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "avro-schema";
  }

  @Test
  @SneakyThrows
  void givenSwaggerTest_whenCompiledAndServerStarted_thenSwaggerEndpointsAvailable() {
    // Compile and start the server
    compileAndStartServer(testDir);

    // Test Swagger JSON endpoint
    testSwaggerJsonEndpoint();

    // Test Swagger UI endpoint
    testSwaggerUIEndpoint();
  }

  @SneakyThrows
  private void testSwaggerJsonEndpoint() {
    var swaggerJsonUrl = getBaseUrl() + "/v1/swagger";
    var request = new HttpGet(swaggerJsonUrl);
    var response = sharedHttpClient.execute(request);

    // Verify response status
    assertThat(response.getStatusLine().getStatusCode())
        .as("Swagger JSON endpoint should return 200")
        .isEqualTo(200);

    // Verify content type
    assertThat(response.getEntity().getContentType().getValue())
        .as("Swagger JSON should have correct content type")
        .isEqualTo("application/json");

    // Parse and validate JSON structure
    var responseBody = EntityUtils.toString(response.getEntity());
    var jsonResponse = objectMapper.readTree(responseBody);

    // Verify OpenAPI structure
    assertThat(jsonResponse.has("openapi")).as("Response should contain OpenAPI version").isTrue();
    assertThat(jsonResponse.has("info")).as("Response should contain API info").isTrue();
    assertThat(jsonResponse.has("paths")).as("Response should contain API paths").isTrue();

    // Verify API info
    var info = jsonResponse.get("info");
    assertThat(info.get("title").asText())
        .as("API title should be set")
        .isEqualTo("DataSQRL REST API");
    assertThat(info.get("version").asText()).as("API version should be set").isEqualTo("1.0.0");

    // Verify REST endpoints are documented
    var paths = jsonResponse.get("paths");
    assertThat(paths.has("/v1/rest/queries/Schema"))
        .as("Schema REST endpoint should be documented")
        .isTrue();

    // Verify endpoint details
    var schemaPath = paths.get("/v1/rest/queries/Schema");
    assertThat(schemaPath.has("get")).as("Schema endpoint should have GET method").isTrue();
  }

  @SneakyThrows
  private void testSwaggerUIEndpoint() {
    var swaggerUIUrl = getBaseUrl() + "/v1/swagger-ui";
    var request = new HttpGet(swaggerUIUrl);
    var response = sharedHttpClient.execute(request);

    // Verify response status
    assertThat(response.getStatusLine().getStatusCode())
        .as("Swagger UI endpoint should return 200")
        .isEqualTo(200);

    // Verify content type
    assertThat(response.getEntity().getContentType().getValue())
        .as("Swagger UI should have HTML content type")
        .isEqualTo("text/html");

    // Verify HTML content contains Swagger UI
    var responseBody = EntityUtils.toString(response.getEntity());
    assertThat(responseBody)
        .as("Response should contain Swagger UI HTML")
        .contains("swagger-ui")
        .contains("SwaggerUIBundle")
        .contains("DataSQRL REST API");
  }

  @Test
  @SneakyThrows
  void givenDisabledSwaggerConfig_whenCompiledAndServerStarted_thenSwaggerEndpointsNotAvailable() {
    // Compile the script first
    compileSqrlProject(testDir);

    // Modify the server configuration to disable Swagger
    disableSwaggerInConfiguration(testDir);

    // Start the server with modified configuration
    startGraphQLServer(testDir);

    // Test that Swagger JSON endpoint returns 404
    testSwaggerEndpointNotAvailable("/swagger");

    // Test that Swagger UI endpoint returns 404
    testSwaggerEndpointNotAvailable("/swagger-ui");
  }

  @SneakyThrows
  private void disableSwaggerInConfiguration(Path testDir) {
    var vertxConfigPath = testDir.resolve("build/deploy/plan/vertx-config.json");

    // Read the existing configuration
    var configContent = Files.readString(vertxConfigPath);
    var mapper = new ObjectMapper();
    var configNode = (ObjectNode) mapper.readTree(configContent);

    // Add disabled Swagger configuration
    var swaggerConfig = mapper.createObjectNode();
    swaggerConfig.put("enabled", false);
    swaggerConfig.put("endpoint", "/swagger");
    swaggerConfig.put("uiEndpoint", "/swagger-ui");
    configNode.set("swaggerConfig", swaggerConfig);

    // Write back the modified configuration
    Files.writeString(
        vertxConfigPath, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(configNode));
  }

  @SneakyThrows
  private void testSwaggerEndpointNotAvailable(String endpoint) {
    var url = getBaseUrl() + endpoint;
    var request = new HttpGet(url);
    var response = sharedHttpClient.execute(request);

    // Verify response status is 404 (not found)
    assertThat(response.getStatusLine().getStatusCode())
        .as("Swagger endpoint %s should return 404 when disabled", endpoint)
        .isEqualTo(404);
  }
}
