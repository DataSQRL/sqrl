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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
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

    assertThat(serverContainer.getLogs()).contains("REQUEST BODY");
  }

  @Test
  @SneakyThrows
  void givenTraceRequestsEnabled_whenHttpRequestsMade_thenLogPathAndBody() {
    compileAndStartServer(
        "myudf.sqrl", testDir, container -> container.withEnv("SQRL_TRACE_REQUESTS", "true"));

    var testQuery = "{\"query\":\"query { __typename }\"}";
    var response = executeGraphQLQuery(testQuery);
    validateBasicGraphQLResponse(response);

    assertThat(serverContainer.getLogs())
        .contains("REQUEST BODY")
        .contains(testQuery)
        .contains("/graphql");

    var randomPath = "/random/test/path/12345";
    var testBody = "{\"test\":\"data\",\"random\":\"content\"}";

    var randomRequest = new HttpPost(getBaseUrl() + randomPath);
    randomRequest.setEntity(new StringEntity(testBody, ContentType.APPLICATION_JSON));

    sharedHttpClient.execute(randomRequest);

    assertThat(serverContainer.getLogs())
        .contains("REQUEST BODY")
        .contains(testBody)
        .contains(randomPath);

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

    assertThat(serverContainer.getLogs())
        .contains("INCOMING REQUEST")
        .contains("Method: GET")
        .contains("URI: /graphiql/");
  }
}
