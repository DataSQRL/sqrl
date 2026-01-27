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
package com.datasqrl.graphql.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.graphql.server.ModelContainer;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestBridgeControllerTest {

  private RootGraphqlModel model;

  @BeforeEach
  @SneakyThrows
  void givenTestModel_whenSetup_thenModelIsLoaded() {
    model =
        new ObjectMapper()
            .readValue(Resources.getResource("testdata/spring-rest.json"), ModelContainer.class)
            .models
            .get("v1");
  }

  @Test
  void givenQueryParamsInUri_whenConvertUriTemplateToPath_thenQueryParamsRemoved() {
    var uriTemplate = "queries/HighTempAlert{?offset,limit}";
    var path = RestBridgeController.convertUriTemplateToPath(uriTemplate);

    assertThat(path).isEqualTo("queries/HighTempAlert");
  }

  @Test
  void givenPathParamsInUri_whenConvertUriTemplateToPath_thenPathParamsConverted() {
    var uriTemplate = "queries/{sensorid}/maxTemp{?offset,limit}";
    var path = RestBridgeController.convertUriTemplateToPath(uriTemplate);

    assertThat(path).isEqualTo("queries/{sensorid}/maxTemp");
  }

  @Test
  void givenSimpleUri_whenConvertUriTemplateToPath_thenPathUnchanged() {
    var uriTemplate = "mutations/SensorReading";
    var path = RestBridgeController.convertUriTemplateToPath(uriTemplate);

    assertThat(path).isEqualTo("mutations/SensorReading");
  }

  @Test
  void givenIntegerProperty_whenExtractAndConvertQueryParams_thenConvertsToLong() {
    var operation = getHighTempOperation();
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("offset", "10");
    queryParams.put("limit", "20");

    Map<String, Object> parameters = new HashMap<>();
    RestBridgeController.extractAndConvertQueryParams(queryParams, operation, parameters);

    assertThat(parameters).containsEntry("offset", 10L);
    assertThat(parameters).containsEntry("limit", 20L);
  }

  @Test
  void givenPathParams_whenExtractAndConvertPathParams_thenConvertsCorrectly() {
    var operation = getSensorMaxOperation();
    Map<String, String> pathParams = new HashMap<>();
    pathParams.put("sensorid", "123");

    Map<String, Object> parameters = new HashMap<>();
    RestBridgeController.extractAndConvertPathParams(pathParams, operation, parameters);

    assertThat(parameters).containsEntry("sensorid", 123L);
  }

  @Test
  void givenBodyWithNestedObject_whenExtractBody_thenExtractsCorrectly() {
    Map<String, Object> body = new HashMap<>();
    body.put(
        "event",
        List.of(
            Map.of("sensorid", 1, "temperature", 25.5),
            Map.of("sensorid", 2, "temperature", 30.0)));

    Map<String, Object> parameters = new HashMap<>();
    RestBridgeController.extractBody(body, parameters);

    assertThat(parameters).containsKey("event");
    assertThat(parameters.get("event")).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> eventList = (List<Map<String, Object>>) parameters.get("event");
    assertThat(eventList).hasSize(2);
  }

  @Test
  void givenModel_whenGetOperations_thenReturnsAllOperations() {
    assertThat(model.getOperations()).hasSize(4);
  }

  @Test
  void givenGetOperation_whenCheckRestMethod_thenReturnsGet() {
    var operation = getHighTempOperation();
    assertThat(operation.getRestMethod().name()).isEqualTo("GET");
  }

  @Test
  void givenPostOperation_whenCheckRestMethod_thenReturnsPost() {
    var operation = addSensorReadingOperation();
    assertThat(operation.getRestMethod().name()).isEqualTo("POST");
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
