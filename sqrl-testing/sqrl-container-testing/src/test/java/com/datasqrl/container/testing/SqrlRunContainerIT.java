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

import java.time.Duration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class SqrlRunContainerIT extends SqrlContainerTestBase {

  private GenericContainer<?> runContainer;

  @Override
  protected String getTestCaseName() {
    return "avro-schema";
  }

  @Test
  @SneakyThrows
  void givenAvroSchemaScript_whenRunCommandExecuted_thenServerStartsAndRespondsToGraphQL() {
    runContainer =
        createRunContainer("avro-schema.sqrl")
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(HTTP_SERVER_PORT)
                    .forStatusCode(204)
                    .withStartupTimeout(Duration.ofSeconds(30)));

    runContainer.start();

    var logs = runContainer.getLogs();
    log.info("SQRL run command executed successfully");
    log.info("Container logs:\n{}", logs);

    assertThat(logs).contains("GraphQL verticle deployed successfully");

    testGraphQLEndpoint();
    testHealthEndpoint();

    log.info("All endpoint validations passed successfully");
  }

  private void testGraphQLEndpoint() throws Exception {
    var baseUrl = "http://localhost:" + runContainer.getMappedPort(HTTP_SERVER_PORT);
    var graphqlEndpoint = baseUrl + "/graphql";

    var response = executePostRequest(graphqlEndpoint, "{\"query\":\"query { __typename }\"}");
    validateBasicGraphQLResponse(response);
  }

  private void testHealthEndpoint() throws Exception {
    var baseUrl = "http://localhost:" + runContainer.getMappedPort(HTTP_SERVER_PORT);
    var healthEndpoint = baseUrl + "/health";

    var response = executeGetRequest(healthEndpoint);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(204);
  }

  @Override
  protected void cleanupContainers() {
    super.cleanupContainers();
    if (runContainer != null && runContainer.isRunning()) {
      runContainer.stop();
      runContainer = null;
    }
  }
}
