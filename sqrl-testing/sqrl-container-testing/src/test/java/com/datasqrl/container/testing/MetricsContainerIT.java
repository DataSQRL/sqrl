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
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;

public class MetricsContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "udf";
  }

  @Test
  @SneakyThrows
  void givenRunningServer_whenAccessingMetricsEndpoint_thenReturnsMetrics() {
    compileAndStartServer("myudf.sqrl");

    var response = executeGetRequest(getMetricsEndpoint());

    var statusCode = response.getStatusLine().getStatusCode();

    if (statusCode == 200) {
      validatePrometheusMetrics(response);
    } else {
      throw new AssertionError(
          "Unexpected status code for /metrics endpoint: "
              + statusCode
              + serverContainer.getLogs());
    }
  }

  private void validatePrometheusMetrics(HttpResponse response) throws Exception {
    var responseBody = EntityUtils.toString(response.getEntity());

    assertThat(responseBody)
        .as("Metrics response should contain Prometheus-formatted metrics")
        .isNotEmpty()
        .contains("# HELP")
        .contains("# TYPE");
  }

  @Test
  @SneakyThrows
  void givenRunningServer_whenAccessingHealthEndpoint_thenReturnsHealthStatus() {
    compileAndStartServer("myudf.sqrl");

    var response = executeGetRequest(getHealthEndpoint());

    var statusCode = response.getStatusLine().getStatusCode();

    assertThat(statusCode)
        .as("Health endpoint should return either 200 (with JSON) or 204 (no content)")
        .isIn(200, 204);

    if (statusCode == 200) {
      validateHealthJson(response);
    }
  }

  private void validateHealthJson(HttpResponse response) throws Exception {
    var responseBody = EntityUtils.toString(response.getEntity());
    var jsonResponse = objectMapper.readTree(responseBody);

    assertThat(jsonResponse.has("status")).isTrue();
    assertThat(jsonResponse.get("status").asText()).isEqualTo("UP");
  }
}
