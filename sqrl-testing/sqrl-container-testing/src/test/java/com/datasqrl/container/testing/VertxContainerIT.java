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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.SneakyThrows;

public class VertxContainerIT extends SqrlContainerTestBase {

  private static final Logger logger = LoggerFactory.getLogger(VertxContainerIT.class);

  @AfterEach
  void tearDown() {
    cleanupContainers();
  }

  @Test
  @SneakyThrows
  void givenUdfScript_whenCompiledAndServerStarted_thenApiRespondsCorrectly() {
    var testDir = getTestResourcePath("udf").toAbsolutePath().toString();

    logger.info("Running Vert.x container test");

          compileSqrlScript("myudf.sqrl", testDir);

          startGraphQLServer(testDir);

          var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}");

          assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

          var responseBody = EntityUtils.toString(response.getEntity());
          var jsonResponse = objectMapper.readTree(responseBody);

          assertThat(jsonResponse.has("data")).isTrue();
          assertThat(jsonResponse.get("data").has("__typename")).isTrue();
          assertThat(jsonResponse.get("data").get("__typename").asText()).isEqualTo("Query");

          logger.info("Vert.x container test completed successfully");
  }
}
