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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

@Slf4j
public class SqrlRunContainerIT {

  @RegisterExtension static SqrlContainerExtension sqrl = new SqrlContainerExtension("avro-schema");

  private GenericContainer<?> runContainer;

  @AfterEach
  void tearDown() {
    if (runContainer != null && runContainer.isRunning()) {
      runContainer.stop();
      runContainer = null;
    }
  }

  @Test
  @SneakyThrows
  void givenAvroSchemaScript_whenRunCommandExecuted_thenServerStartsAndRespondsToGraphQL() {
    // Start the run container which compiles and runs the server all in one
    runContainer =
        sqrl.createCmdContainer()
            .withCommand("run")
            .withExposedPorts(SqrlContainerExtension.HTTP_SERVER_PORT)
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(SqrlContainerExtension.HTTP_SERVER_PORT)
                    .forStatusCode(204)
                    .withStartupTimeout(Duration.ofSeconds(30)));

    runContainer.start();

    var logs = runContainer.getLogs();
    log.info("SQRL run command executed successfully");
    log.info("Container logs:\n{}", logs);

    // Verify server started successfully
    assertThat(logs).contains("GraphQL verticle deployed successfully");

    // Test GraphQL endpoint
    var baseUrl =
        "http://localhost:" + runContainer.getMappedPort(SqrlContainerExtension.HTTP_SERVER_PORT);
    var graphqlEndpoint = baseUrl + "/v1/graphql";

    try (var response =
        executeGraphQLQueryToRunContainer(
            graphqlEndpoint, "{\"query\":\"query { __typename }\"}")) {
      sqrl.validateBasicGraphQLResponse(response);
    }

    // Verify health endpoint
    var healthEndpoint = baseUrl + "/health";
    try (var healthResponse = executeHealthCheck(healthEndpoint)) {
      assertThat(healthResponse.getStatusLine().getStatusCode()).isEqualTo(204);
    }

    log.info("All endpoint validations passed successfully");
  }

  private CloseableHttpResponse executeGraphQLQueryToRunContainer(String endpoint, String query)
      throws Exception {
    var request = new HttpPost(endpoint);
    request.setEntity(new StringEntity(query, ContentType.APPLICATION_JSON));
    return sqrl.getHttpClient().execute(request);
  }

  private CloseableHttpResponse executeHealthCheck(String endpoint) throws Exception {
    var request = new HttpGet(endpoint);
    return sqrl.getHttpClient().execute(request);
  }
}
