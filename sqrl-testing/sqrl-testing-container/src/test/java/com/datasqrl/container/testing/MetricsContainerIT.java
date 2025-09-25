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

public class MetricsContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "udf";
  }

  @Test
  void givenRunningServer_whenAccessingMetricsEndpoint_thenReturnsMetrics() {
    var metricsData = getMetricsResult();

    // Validate Prometheus format
    assertThat(metricsData)
        .as("Metrics response should contain Prometheus-formatted metrics")
        .isNotEmpty()
        .contains("# HELP")
        .contains("# TYPE");
  }

  @Test
  void givenRunningServer_whenAccessingMetricsEndpoint_thenReturnsJvmMetrics() {
    var metricsData = getMetricsResult();

    // Validate Prometheus format
    assertThat(metricsData)
        .as("Metrics response should contain JVM metrics")
        .isNotEmpty()
        .contains("# TYPE jvm_buffer_")
        .contains("# TYPE jvm_gc_")
        .contains("# TYPE jvm_memory_")
        .contains("# TYPE jvm_threads_")
        .contains("# TYPE process_");
  }

  @Test
  void givenRunningServer_whenAccessingMetricsEndpoint_thenReturnsDbPoolMetrics() {
    var metricsData = getMetricsResult();

    // Validate Prometheus format
    assertThat(metricsData)
        .as("Metrics response should contain DB pool metrics")
        .isNotEmpty()
        .contains("# TYPE vertx_pool_")
        .contains("pool_name=\"postgres-pool\"");
  }

  @Test
  @SneakyThrows
  void givenRunningServer_whenAccessingHealthEndpoint_thenReturnsHealthStatus() {
    compileAndStartServer("myudf.sqrl", testDir);

    var response = sharedHttpClient.execute(new HttpGet(getHealthEndpoint()));

    var statusCode = response.getStatusLine().getStatusCode();

    // Health endpoint can return either 200 (with JSON) or 204 (no content) - both indicate healthy
    assertThat(statusCode)
        .as("Health endpoint should return either 200 (with JSON) or 204 (no content)")
        .isIn(200, 204);

    if (statusCode == 200) {
      var responseBody = EntityUtils.toString(response.getEntity());
      var jsonResponse = objectMapper.readTree(responseBody);

      assertThat(jsonResponse.has("status")).isTrue();
      assertThat(jsonResponse.get("status").asText()).isEqualTo("UP");
    }
    // 204 No Content indicates healthy server with no registered health checks
  }

  @SneakyThrows
  private String getMetricsResult() {
    compileAndStartServer("myudf.sqrl", testDir);

    var response = sharedHttpClient.execute(new HttpGet(getMetricsEndpoint()));

    var statusCode = response.getStatusLine().getStatusCode();

    if (statusCode == 200) {
      // Metrics endpoint is available
      return EntityUtils.toString(response.getEntity());

    } else {
      throw new AssertionError(
          "Unexpected status code for /metrics endpoint: "
              + statusCode
              + serverContainer.getLogs());
    }
  }
}
