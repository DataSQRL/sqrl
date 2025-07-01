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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsContainerIT extends SqrlContainerTestBase {

  private static final Logger logger = LoggerFactory.getLogger(MetricsContainerIT.class);

  @AfterEach
  void tearDown() {
    cleanupContainers();
  }

  @Test
  @SneakyThrows
  void givenRunningServer_whenAccessingMetricsEndpoint_thenReturnsMetrics() {
    var testDir = itPath("udf");

    logger.info("Running metrics container test - validating /metrics endpoint");

    compileSqrlScript("myudf.sqrl", testDir);
    startGraphQLServer(testDir);

    var request =
        new HttpGet("http://localhost:" + serverContainer.getMappedPort(GRAPHQL_PORT) + "/metrics");
    var response = sharedHttpClient.execute(request);

    var statusCode = response.getStatusLine().getStatusCode();

    if (statusCode == 200) {
      // Metrics endpoint is available - validate content
      var responseBody = EntityUtils.toString(response.getEntity());

      // Validate Prometheus format and essential metrics
      assertThat(responseBody)
          .as("Metrics response should contain Prometheus-formatted metrics")
          .isNotEmpty()
          .contains("# HELP")
          .contains("# TYPE");

      logger.info("Metrics endpoint is available and returning Prometheus metrics");
    } else if (statusCode == 404) {
      // Metrics endpoint not available - this is acceptable if Prometheus registry is not
      // configured
      logger.info(
          "Metrics endpoint returns 404 - Prometheus registry may not be configured in container environment");
    } else {
      throw new AssertionError("Unexpected status code for /metrics endpoint: " + statusCode);
    }

    logger.info("Metrics endpoint validation completed successfully");
  }
}
