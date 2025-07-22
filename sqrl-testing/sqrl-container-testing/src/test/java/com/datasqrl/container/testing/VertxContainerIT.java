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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.junit.jupiter.api.Test;

public class VertxContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "udf";
  }

  @Test
  @SneakyThrows
  void givenUdfScript_whenCompiledAndServerStarted_thenApiRespondsCorrectly() {
    compileAndStartServer("myudf.sqrl", testDir);

    var response = executeGraphQLQuery("{\"query\":\"query { __typename }\"}");
    validateBasicGraphQLResponse(response);
  }

  @Test
  @SneakyThrows
  void givenTraceRequestsEnabled_whenGraphQLRequestSent_thenRequestBodyLoggedInServerLogs() {
    compileAndStartServer(
        "myudf.sqrl", testDir, container -> container.withEnv("DATASQRL_TRACE_REQUESTS", "true"));

    var testQuery = "{\"query\":\"query { __typename }\"}";
    var response = executeGraphQLQuery(testQuery);
    validateBasicGraphQLResponse(response);

    assertThat(serverContainer.getLogs()).contains("REQUEST BODY");
  }

  @Test
  @SneakyThrows
  void givenTraceRequestsEnabled_whenPostToRandomPath_thenPathAndBodyLoggedInServerLogs() {
    compileAndStartServer(
        "myudf.sqrl", testDir, container -> container.withEnv("DATASQRL_TRACE_REQUESTS", "true"));

    var randomPath = "/random/test/path/12345";
    var testBody = "{\"test\":\"data\",\"random\":\"content\"}";

    var request = new HttpPost(getBaseUrl() + randomPath);
    request.setEntity(new StringEntity(testBody, ContentType.APPLICATION_JSON));

    sharedHttpClient.execute(request);

    assertThat(serverContainer.getLogs()).contains("REQUEST BODY")
    .contains(testBody)
    .contains(randomPath);
  }
}
