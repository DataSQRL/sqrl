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
    compileAndStartServer("avro-schema.sqrl");

    testSwaggerJsonEndpoint();
    testSwaggerUIEndpoint();
  }

  @SneakyThrows
  private void testSwaggerJsonEndpoint() {
    var response = executeGetRequest(getSwaggerEndpoint());

    validateJsonResponse(response, "application/json");

    var responseBody = EntityUtils.toString(response.getEntity());
    var jsonResponse = objectMapper.readTree(responseBody);

    validateOpenApiStructure(jsonResponse);
    validateApiInfo(jsonResponse);
    validateRestEndpoints(jsonResponse);
  }

  private void validateOpenApiStructure(com.fasterxml.jackson.databind.JsonNode jsonResponse) {
    assertThat(jsonResponse.has("openapi")).as("Response should contain OpenAPI version").isTrue();
    assertThat(jsonResponse.has("info")).as("Response should contain API info").isTrue();
    assertThat(jsonResponse.has("paths")).as("Response should contain API paths").isTrue();
  }

  private void validateApiInfo(com.fasterxml.jackson.databind.JsonNode jsonResponse) {
    var info = jsonResponse.get("info");
    assertThat(info.get("title").asText())
        .as("API title should be set")
        .isEqualTo("DataSQRL REST API");
    assertThat(info.get("version").asText()).as("API version should be set").isEqualTo("1.0.0");
  }

  private void validateRestEndpoints(com.fasterxml.jackson.databind.JsonNode jsonResponse) {
    var paths = jsonResponse.get("paths");
    assertThat(paths.has("/queries/Schema"))
        .as("Schema REST endpoint should be documented")
        .isTrue();

    var schemaPath = paths.get("/queries/Schema");
    assertThat(schemaPath.has("get")).as("Schema endpoint should have GET method").isTrue();
  }

  @SneakyThrows
  private void testSwaggerUIEndpoint() {
    var response = executeGetRequest(getSwaggerUIEndpoint());

    validateHtmlResponse(response);

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
    compileSqrlScript("avro-schema.sqrl", testDir);
    disableSwaggerInConfiguration(testDir);
    startGraphQLServer(testDir);

    testSwaggerEndpointNotAvailable("/swagger");
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
    var response = executeGetRequest(getBaseUrl() + endpoint);
    assertNotFoundResponse(response);
  }
}
