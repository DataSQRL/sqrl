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
package com.datasqrl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.util.JsonUtils;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQL;
import graphql.GraphqlErrorBuilder;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class McpBridgeVerticleResourcesTest {

  private static final String OPERATIONS =
      """
      [
        {
          "function": {"name": "AllPeople", "parameters": {"type": "object", "properties": {}}},
          "apiQuery": {"query": "query AllPeople { Person { name } }", "queryName": "AllPeople", "operationType": "QUERY"},
          "mcpMethod": "RESOURCE", "restMethod": "GET", "uriTemplate": "queries/AllPeople"
        },
        {
          "function": {
            "name": "PersonByAge", "description": "People of a given age.",
            "parameters": {"type": "object", "properties": {"age": {"type": "integer"}}, "required": ["age"]}
          },
          "apiQuery": {"query": "query PersonByAge($age: Int!) { PersonByAge(age: $age) { name } }", "queryName": "PersonByAge", "operationType": "QUERY"},
          "mcpMethod": "RESOURCE", "restMethod": "GET", "uriTemplate": "/queries/personByAge/{age}"
        },
        {
          "function": {
            "name": "People",
            "parameters": {"type": "object", "properties": {"limit": {"type": "integer"}, "offset": {"type": "integer"}}, "required": []}
          },
          "apiQuery": {"query": "query People($limit: Int, $offset: Int) { Person(limit: $limit, offset: $offset) { name } }", "queryName": "People", "operationType": "QUERY"},
          "mcpMethod": "RESOURCE", "restMethod": "GET", "uriTemplate": "queries/People{?limit,offset}"
        },
        {
          "function": {"name": "GetPerson", "parameters": {"type": "object", "properties": {"limit": {"type": "integer"}}}},
          "apiQuery": {"query": "query GetPerson($limit: Int) { Person(limit: $limit) { name } }", "queryName": "GetPerson", "operationType": "QUERY"},
          "mcpMethod": "TOOL", "restMethod": "GET", "uriTemplate": "queries/GetPerson{?limit}"
        }
      ]
      """;

  private final GraphQL graphQL = mock(GraphQL.class);
  private final RoutingContext ctx = mock(RoutingContext.class);
  private ServerConfig config;
  private RootGraphQLModel model;

  @BeforeEach
  @SneakyThrows
  void setUp() {
    config = new ServerConfig();
    model = mock(RootGraphQLModel.class);
    List<ApiOperation> operations =
        JsonUtils.MAPPER.readValue(
            OPERATIONS,
            JsonUtils.MAPPER
                .getTypeFactory()
                .constructCollectionType(List.class, ApiOperation.class));
    when(model.getOperations()).thenReturn(operations);
  }

  @Test
  void givenResourceOperations_whenListingResources_thenUrisAreAbsoluteWithVersion() {
    var resources =
        request("resources/list", null).getJsonObject("result").getJsonArray("resources");

    assertThat(resources).hasSize(1);
    assertThat(resources.getJsonObject(0).getMap())
        .containsEntry("uri", "sqrl://v1/queries/AllPeople")
        .containsEntry("name", "AllPeople")
        .containsEntry("mimeType", "application/json");
  }

  @Test
  void givenResourceOperations_whenListingTemplates_thenReturnsUriTemplateField() {
    var templates =
        request("resources/templates/list", null)
            .getJsonObject("result")
            .getJsonArray("resourceTemplates");

    assertThat(templates.stream().map(t -> ((JsonObject) t).getString("uriTemplate")))
        .containsExactly(
            "sqrl://v1/queries/personByAge/{age}", "sqrl://v1/queries/People{?limit,offset}");
    assertThat(templates.stream().map(t -> ((JsonObject) t).containsKey("uri")))
        .containsOnly(false);
    assertThat(templates.getJsonObject(0).getString("description"))
        .isEqualTo("People of a given age.");
  }

  @Test
  void givenCustomPrefix_whenListingResources_thenUsesPrefix() {
    config.setMcpResourceUriPrefix("acme://data/{version}/");

    var resource =
        request("resources/list", null)
            .getJsonObject("result")
            .getJsonArray("resources")
            .getJsonObject(0);

    assertThat(resource.getString("uri")).isEqualTo("acme://data/v1/queries/AllPeople");
  }

  @Test
  void givenStaticResourceUri_whenReading_thenRunsQueryAndReturnsUnnestedJson() {
    var input = mockExecution(Map.of("Person", List.of(Map.of("name", "Ada"))));

    var result = read("sqrl://v1/queries/AllPeople").getJsonObject("result");

    var content = result.getJsonArray("contents").getJsonObject(0);
    assertThat(content.getString("uri")).isEqualTo("sqrl://v1/queries/AllPeople");
    assertThat(content.getString("mimeType")).isEqualTo("application/json");
    assertThat(content.getString("text")).isEqualTo("[{\"name\":\"Ada\"}]");
    assertThat(input.getValue().getOperationName()).isEqualTo("AllPeople");
    assertThat(input.getValue().getVariables()).isEmpty();
  }

  @Test
  void givenPathTemplateUri_whenReading_thenConvertsPathParameter() {
    var input = mockExecution(Map.of("PersonByAge", List.of()));

    var content =
        read("sqrl://v1/queries/personByAge/30")
            .getJsonObject("result")
            .getJsonArray("contents")
            .getJsonObject(0);

    assertThat(content.getString("uri")).isEqualTo("sqrl://v1/queries/personByAge/30");
    assertThat(input.getValue().getOperationName()).isEqualTo("PersonByAge");
    assertThat(input.getValue().getVariables()).containsExactlyEntriesOf(Map.of("age", 30L));
  }

  @Test
  void givenQueryTemplateUri_whenReading_thenConvertsQueryParameters() {
    var input = mockExecution(Map.of("Person", List.of()));

    read("sqrl://v1/queries/People?limit=1&offset=2&unknown=x");

    assertThat(input.getValue().getOperationName()).isEqualTo("People");
    assertThat(input.getValue().getVariables())
        .containsOnly(Map.entry("limit", 1L), Map.entry("offset", 2L));
  }

  @Test
  void givenUnknownUri_whenReading_thenReturnsResourceNotFound() {
    var error = read("nothing").getJsonObject("error");

    assertThat(error.getInteger("code")).isEqualTo(-32002);
    assertThat(error.getString("message")).isEqualTo("Resource not found");
    assertThat(error.getJsonObject("data").getString("uri")).isEqualTo("nothing");
    verify(graphQL, never()).executeAsync(any(ExecutionInput.class));
  }

  @Test
  void givenRelativeUri_whenReading_thenReturnsResourceNotFound() {
    assertThat(read("queries/AllPeople").getJsonObject("error").getInteger("code"))
        .isEqualTo(-32002);
  }

  @Test
  void givenInvalidParameter_whenReading_thenReturnsInvalidParams() {
    var error = read("sqrl://v1/queries/personByAge/abc").getJsonObject("error");

    assertThat(error.getInteger("code")).isEqualTo(-32602);
    assertThat(error.getString("message")).contains("age");
    verify(graphQL, never()).executeAsync(any(ExecutionInput.class));
  }

  @Test
  void givenInvalidPercentEncoding_whenReading_thenReturnsInvalidParams() {
    assertThat(read("sqrl://v1/queries/personByAge/%zz").getJsonObject("error").getInteger("code"))
        .isEqualTo(-32602);
  }

  @Test
  void givenMissingUri_whenReading_thenReturnsInvalidParams() {
    var error = request("resources/read", "{}").getJsonObject("error");

    assertThat(error.getInteger("code")).isEqualTo(-32602);
  }

  @Test
  void givenGraphQLError_whenReading_thenReturnsErrorResponse() {
    var executionResult =
        ExecutionResultImpl.newExecutionResult()
            .addError(GraphqlErrorBuilder.newError().message("relation does not exist").build())
            .build();
    mockExecution(executionResult);

    var response = read("sqrl://v1/queries/AllPeople");

    assertThat(response.containsKey("result")).isFalse();
    assertThat(response.getJsonObject("error").getInteger("code")).isEqualTo(-32603);
    assertThat(response.getJsonObject("error").getString("message"))
        .contains("relation does not exist");
  }

  @Test
  void givenResourceOperations_whenListingTools_thenOnlyToolsAreListed() {
    var tools = request("tools/list", null).getJsonObject("result").getJsonArray("tools");

    assertThat(tools.stream().map(t -> ((JsonObject) t).getString("name")))
        .containsExactly("GetPerson");
  }

  private ArgumentCaptor<ExecutionInput> mockExecution(Object data) {
    return mockExecution(ExecutionResultImpl.newExecutionResult().data(data).build());
  }

  private ArgumentCaptor<ExecutionInput> mockExecution(ExecutionResult executionResult) {
    var captor = ArgumentCaptor.forClass(ExecutionInput.class);
    when(graphQL.executeAsync(captor.capture()))
        .thenReturn(CompletableFuture.completedFuture(executionResult));
    return captor;
  }

  private JsonObject read(String uri) {
    return request("resources/read", new JsonObject().put("uri", uri).encode());
  }

  @SneakyThrows
  private JsonObject request(String method, String paramsJson) {
    var graphQLServerVerticle = mock(GraphQLServerVerticle.class);
    when(graphQLServerVerticle.getGraphQLEngine()).thenReturn(graphQL);
    var verticle =
        new McpBridgeVerticle(
            mock(Router.class), config, "v1", model, List.of(), graphQLServerVerticle);
    var request =
        JsonUtils.MAPPER
            .createObjectNode()
            .put("jsonrpc", "2.0")
            .put("id", 1)
            .put("method", method)
            .set("params", paramsJson == null ? null : JsonUtils.MAPPER.readTree(paramsJson));
    return verticle.handleRequest(ctx, request).toCompletionStage().toCompletableFuture().get();
  }
}
