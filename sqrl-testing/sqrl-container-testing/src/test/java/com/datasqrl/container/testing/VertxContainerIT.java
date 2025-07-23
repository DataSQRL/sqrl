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
import java.util.regex.Pattern;
import lombok.SneakyThrows;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.awaitility.Awaitility;
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
        "myudf.sqrl", testDir, container -> container.withEnv("SQRL_TRACE_REQUESTS", "true"));

    var testQuery = "{\"query\":\"query { __typename }\"}";
    var response = executeGraphQLQuery(testQuery);
    validateBasicGraphQLResponse(response);

    assertTraceLogContains("REQUEST BODY");
  }

  @Test
  @SneakyThrows
  void givenTraceRequestsEnabled_whenHttpRequestsMade_thenLogPathAndBody() {
    compileAndStartServer(
        "myudf.sqrl", testDir, container -> container.withEnv("SQRL_TRACE_REQUESTS", "true"));

    var testQuery = "{\"query\":\"query { __typename }\"}";
    var response = executeGraphQLQuery(testQuery);
    validateBasicGraphQLResponse(response);

    assertTraceLogContains("REQUEST BODY", testQuery, "/graphql");

    var randomPath = "/random/test/path/12345";
    var testBody = "{\"test\":\"data\",\"random\":\"content\"}";

    var randomRequest = new HttpPost(getBaseUrl() + randomPath);
    randomRequest.setEntity(new StringEntity(testBody, ContentType.APPLICATION_JSON));

    sharedHttpClient.execute(randomRequest);

    assertTraceLogContains("REQUEST BODY", testBody, randomPath);

    var graphiqlResponse = sharedHttpClient.execute(new HttpGet(getBaseUrl() + "/graphiql/"));

    assertThat(graphiqlResponse.getStatusLine().getStatusCode()).isEqualTo(200);
    assertThat(graphiqlResponse.getFirstHeader("Content-Type").getValue()).contains("text/html");

    var graphiql = EntityUtils.toString(graphiqlResponse.getEntity());

    assertThat(graphiql)
        .contains("<!doctype html>")
        .contains("<html lang=\"en\">")
        .contains("<title>GraphiQL</title>")
        .contains("window.VERTX_GRAPHIQL_CONFIG")
        .contains("\"httpEnabled\":true")
        .contains("\"graphQLUri\":\"/graphql\"")
        .contains("\"graphQLWSEnabled\":true")
        .contains("\"graphQLWSUri\":\"/graphql\"")
        .contains("static/js/main.")
        .contains("static/css/main.")
        .contains("<div id=\"root\"></div>");

    assertTraceLogContains("INCOMING REQUEST", "Method: GET", "URI: /graphiql/");
  }

  @Test
  @SneakyThrows
  void givenTraceRequestsEnabled_whenRequestsMade_thenLogsContainRequestPrefix() {
    compileAndStartServer(
        "myudf.sqrl", testDir, container -> container.withEnv("SQRL_TRACE_REQUESTS", "true"));

    var testQuery = "{\"query\":\"query { __typename }\"}";
    var response = executeGraphQLQuery(testQuery);
    validateBasicGraphQLResponse(response);

    // Verify that log entries contain request ID prefix in the expected format [REQ-...]
    assertTraceLogMatchesPattern("\\[REQ-\\d+-\\d+\\] INCOMING REQUEST");
    assertTraceLogMatchesPattern("\\[REQ-\\d+-\\d+\\] REQUEST BODY");
    assertTraceLogMatchesPattern("\\[REQ-\\d+-\\d+\\] OUTGOING RESPONSE");
  }

  @SneakyThrows
  private void assertTraceLogContains(String... expectedPatterns) {
    // Wait for log file to be created and populated with expected content
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              assertThat(traceLogs()).contains(expectedPatterns);
            });
  }

  @SneakyThrows
  private void assertTraceLogMatchesPattern(String regexPattern) {
    // Wait for log file to be created and populated with expected pattern
    Awaitility.await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(
            () -> {
              var logs = traceLogs();
              var pattern = Pattern.compile(regexPattern);
              assertThat(pattern.matcher(logs).find())
                  .as("Expected log pattern '%s' not found in logs", regexPattern)
                  .isTrue();
            });
  }

  @SneakyThrows
  private String traceLogs() {
    // Get the log file from the container
    var logPath = "/opt/sqrl/logs/request-trace.log";

    // Copy log file from container to temp location and read it
    var result = serverContainer.execInContainer("cat", logPath);
    if (result.getExitCode() != 0) {
      throw new RuntimeException("Failed to read request trace log: " + result.getStderr());
    }

    return result.getStdout();
  }
}
