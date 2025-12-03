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
package com.datasqrl.graphql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.config.ServletConfig;
import com.datasqrl.graphql.server.ModelContainer;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RestBridgeVerticleTest {

  @Mock private Router router;
  @Mock private ServerConfig serverConfig;
  @Mock private ServletConfig servletConfig;
  @Mock private GraphQLServerVerticle graphQLServerVerticle;
  @Mock private GraphQL graphQL;
  @Mock private RoutingContext routingContext;
  @Mock private HttpServerRequest request;
  @Mock private HttpServerResponse response;
  @Mock private ExecutionResult executionResult;
  @Mock private RequestBody requestBody;
  @Mock private Route route;

  @Captor private ArgumentCaptor<ExecutionInput> executionInputCaptor;

  private RootGraphqlModel model;
  private RestBridgeVerticle restBridgeVerticle;
  private Vertx vertx;

  @BeforeEach
  @SneakyThrows
  void given_setupMocks_when_initializingTest_then_preparesTestEnvironment() {
    MockitoAnnotations.openMocks(this);
    vertx = Vertx.vertx();

    // Read RootGraphQLModel from resource
    model =
        new ObjectMapper()
            .readValue(Resources.getResource("testdata/vertx-rest.json"), ModelContainer.class)
            .models
            .get("v1");

    // Setup basic mocks
    when(serverConfig.getServletConfig()).thenReturn(servletConfig);
    when(servletConfig.getRestEndpoint("v1")).thenReturn("/v1/api/rest");
    when(graphQLServerVerticle.getGraphQLEngine()).thenReturn(graphQL);
    when(routingContext.request()).thenReturn(request);
    when(routingContext.response()).thenReturn(response);
    when(response.setStatusCode(any(Integer.class))).thenReturn(response);
    when(response.putHeader(any(String.class), any(String.class))).thenReturn(response);
    when(request.params()).thenReturn(MultiMap.caseInsensitiveMultiMap());
    when(routingContext.pathParams()).thenReturn(new HashMap<>());

    // Mock router methods to return mock Route
    when(router.get(any(String.class))).thenReturn(route);
    when(router.post(any(String.class))).thenReturn(route);
    when(route.handler(any())).thenReturn(route);

    restBridgeVerticle =
        new RestBridgeVerticle(router, serverConfig, "v1", model, List.of(), graphQLServerVerticle);
  }

  @Test
  void given_restEndpointWithGetMethod_when_setupRestEndpoints_then_createsGetRoute() {
    // When
    Promise<Void> promise = Promise.promise();
    restBridgeVerticle.start(promise);

    // Then
    verify(router).get("/v1/api/rest/queries/HighTempAlert");
    assertThat(promise.future().succeeded()).isTrue();
  }

  @Test
  void given_restEndpointWithPostMethod_when_setupRestEndpoints_then_createsPostRoute() {
    // When
    Promise<Void> promise = Promise.promise();
    restBridgeVerticle.start(promise);

    // Then
    verify(router).post("/v1/api/rest/mutations/SensorReading");
    assertThat(promise.future().succeeded()).isTrue();
  }

  @Test
  void given_postRequestWithEventList_when_handlerExecuted_then_executesGraphQLMutation()
      throws Exception {
    // Given - Setup the test data
    var eventList =
        List.of(
            Map.of("sensorid", 1, "temperature", 25.5), Map.of("sensorid", 2, "temperature", 30.0));
    var jsonBody = new JsonObject().put("event", eventList);

    // Mock the request body
    when(requestBody.asJsonObject()).thenReturn(jsonBody);
    when(routingContext.body()).thenReturn(requestBody);

    // Mock the GraphQL execution result
    var mutationResult =
        Map.of(
            "SensorReading",
            List.of(
                Map.of("sensorid", 1, "temperature", 25.5, "event_time", "2023-07-08T12:00:00Z"),
                Map.of("sensorid", 2, "temperature", 30.0, "event_time", "2023-07-08T12:00:00Z")));
    when(executionResult.getData()).thenReturn(mutationResult);
    when(executionResult.getErrors()).thenReturn(List.of());

    // Mock the GraphQL engine to return a completed future
    var completedFuture = CompletableFuture.completedFuture(executionResult);
    when(graphQL.executeAsync(any(ExecutionInput.class))).thenReturn(completedFuture);

    // Setup response mocking for successful completion
    when(response.end(any(String.class))).thenReturn(null);

    // When - Extract parameters and simulate the bridge call directly
    var operation = addSensorReadingOperation();
    Map<String, Object> parameters =
        RestBridgeVerticle.extractParameters(routingContext, operation);

    // Call the bridge method directly to trigger GraphQL execution
    var future = restBridgeVerticle.bridgeRequestToGraphQL(routingContext, operation, parameters);

    // Wait for the future to complete (it should complete immediately with our mock)
    var result = future.result();

    // Then - Verify the parameters were extracted correctly
    assertThat(parameters).containsKey("event");
    assertThat(parameters.get("event")).isInstanceOf(List.class);

    // Verify the GraphQL execution was called with correct parameters
    verify(graphQL).executeAsync(executionInputCaptor.capture());

    ExecutionInput capturedInput = executionInputCaptor.getValue();
    assertThat(capturedInput.getQuery()).contains("mutation SensorReading");
    assertThat(capturedInput.getOperationName()).isEqualTo("SensorReading");
    assertThat(capturedInput.getVariables()).containsKey("event");

    // Verify the extracted event data matches what we sent
    @SuppressWarnings("unchecked")
    List<Object> capturedEventList = (List<Object>) capturedInput.getVariables().get("event");
    assertThat(capturedEventList).hasSize(2);

    // Verify the result data
    assertThat(result).isEqualTo(executionResult);
    assertThat((Object) result.getData()).isEqualTo(mutationResult);
    assertThat(result.getErrors()).isEmpty();
  }

  @Test
  void given_getRequestWithQueryParams_when_extractParameters_then_extractsCorrectParameters() {
    // Given
    var operation = getHighTempOperation();
    var queryParams = MultiMap.caseInsensitiveMultiMap();
    queryParams.add("offset", "10");
    queryParams.add("limit", "20");
    when(request.params()).thenReturn(queryParams);

    // When
    Map<String, Object> parameters =
        RestBridgeVerticle.extractParameters(routingContext, operation);

    // Then
    assertThat(parameters).containsEntry("offset", 10L);
    assertThat(parameters).containsEntry("limit", 20L);
  }

  @Test
  void given_getRequestWithPathParams_when_extractParameters_then_extractsCorrectParameters() {
    // Given
    var operation = getSensorMaxOperation();
    var pathParams = new HashMap<String, String>();
    pathParams.put("sensorid", "123");
    when(routingContext.pathParams()).thenReturn(pathParams);
    var queryParams = MultiMap.caseInsensitiveMultiMap();
    queryParams.add("offset", "10");
    queryParams.add("limit", "5");
    when(request.params()).thenReturn(queryParams);

    // When
    Map<String, Object> parameters =
        RestBridgeVerticle.extractParameters(routingContext, operation);

    // Then
    assertThat(parameters).containsEntry("sensorid", 123L);
    assertThat(parameters).containsEntry("offset", 10L);
    assertThat(parameters).containsEntry("limit", 5L);
  }

  @Test
  void given_postRequestWithJsonBody_when_extractParameters_then_extractsCorrectParameters() {
    // Given
    var jsonBody =
        new JsonObject()
            .put("event", List.of(new JsonObject().put("sensorid", 1).put("temperature", 25.5)));
    when(requestBody.asJsonObject()).thenReturn(jsonBody);
    when(routingContext.body()).thenReturn(requestBody);

    // When
    Map<String, Object> parameters =
        RestBridgeVerticle.extractParameters(routingContext, addSensorReadingOperation());

    // Then
    assertThat(parameters).containsKey("event");
    assertThat(parameters.get("event")).isInstanceOf(List.class);
  }

  @Test
  void given_uriTemplateWithQueryParams_when_convertUriTemplateToRoute_then_removesQueryParams() {
    // When
    Promise<Void> promise = Promise.promise();
    restBridgeVerticle.start(promise);

    // Then - Should create route without query params
    verify(router).get("/v1/api/rest/queries/HighTempAlert");
  }

  @Test
  void given_uriTemplateWithPathParams_when_convertUriTemplateToRoute_then_convertsToVertxParams() {
    // When
    Promise<Void> promise = Promise.promise();
    restBridgeVerticle.start(promise);

    // Then - Should convert {sensorid} to :sensorid
    verify(router).get("/v1/api/rest/queries/:sensorid/maxTemp");
  }

  private ApiOperation getOperationByName(String name) {
    return model.getOperations().stream()
        .filter(op -> op.getName().equalsIgnoreCase(name))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("Operation not found: " + name));
  }

  private ApiOperation getHighTempOperation() {
    return getOperationByName("GetHighTempAlert");
  }

  private ApiOperation getSensorMaxOperation() {
    return getOperationByName("GetSensorMaxTemp");
  }

  private ApiOperation addSensorReadingOperation() {
    return getOperationByName("AddSensorReading");
  }
}
