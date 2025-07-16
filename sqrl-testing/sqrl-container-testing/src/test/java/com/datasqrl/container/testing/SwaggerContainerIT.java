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

import lombok.SneakyThrows;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

public class SwaggerContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "swagger-test";
  }

  @Test
  @SneakyThrows
  void givenSwaggerTest_whenCompiledAndServerStarted_thenSwaggerEndpointsAvailable() {
    // Compile and start the server
    compileAndStartServer("swagger-test.sqrl", testDir);

    // Test Swagger JSON endpoint
    testSwaggerJsonEndpoint();

    // Test Swagger UI endpoint
    testSwaggerUIEndpoint();

    // Test REST API endpoints documented in Swagger
    testRestAPIEndpoints();
  }

  @SneakyThrows
  private void testSwaggerJsonEndpoint() {
    var swaggerJsonUrl = getBaseUrl() + "/swagger";
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
    assertThat(paths.has("/sensors/readings"))
        .as("SecReading REST endpoint should be documented")
        .isTrue();
    assertThat(paths.has("/sensors/maxtemp"))
        .as("SensorMaxTemp REST endpoint should be documented")
        .isTrue();

    // Verify endpoint details
    var readingsPath = paths.get("/sensors/readings");
    assertThat(readingsPath.has("get")).as("Readings endpoint should have GET method").isTrue();

    var maxTempPath = paths.get("/sensors/maxtemp");
    assertThat(maxTempPath.has("get")).as("MaxTemp endpoint should have GET method").isTrue();
  }

  @SneakyThrows
  private void testSwaggerUIEndpoint() {
    var swaggerUIUrl = getBaseUrl() + "/swagger-ui";
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

  @SneakyThrows
  private void testRestAPIEndpoints() {
    // Test SecReading REST endpoint
    var readingsUrl = getBaseUrl() + "/rest/sensors/readings";
    var readingsRequest = new HttpGet(readingsUrl);
    var readingsResponse = sharedHttpClient.execute(readingsRequest);

    assertThat(readingsResponse.getStatusLine().getStatusCode())
        .as("SecReading REST endpoint should return 200")
        .isEqualTo(200);

    var readingsBody = EntityUtils.toString(readingsResponse.getEntity());
    var readingsJson = objectMapper.readTree(readingsBody);

    assertThat(readingsJson.has("data")).as("REST response should contain data field").isTrue();

    // Test SensorMaxTemp REST endpoint
    var maxTempUrl = getBaseUrl() + "/rest/sensors/maxtemp";
    var maxTempRequest = new HttpGet(maxTempUrl);
    var maxTempResponse = sharedHttpClient.execute(maxTempRequest);

    assertThat(maxTempResponse.getStatusLine().getStatusCode())
        .as("SensorMaxTemp REST endpoint should return 200")
        .isEqualTo(200);

    var maxTempBody = EntityUtils.toString(maxTempResponse.getEntity());
    var maxTempJson = objectMapper.readTree(maxTempBody);

    assertThat(maxTempJson.has("data")).as("REST response should contain data field").isTrue();

    // Test REST endpoint with query parameters
    var readingsWithParamsUrl = getBaseUrl() + "/rest/sensors/readings?limit=5&offset=0";
    var paramsRequest = new HttpGet(readingsWithParamsUrl);
    var paramsResponse = sharedHttpClient.execute(paramsRequest);

    assertThat(paramsResponse.getStatusLine().getStatusCode())
        .as("REST endpoint with parameters should return 200")
        .isEqualTo(200);
  }
}
