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
package com.datasqrl.ai.converter;

import static com.datasqrl.ai.converter.GraphQLSchemaConverterConfig.ignorePrefix;
import static graphql.Assert.assertFalse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.ai.TestUtil;
import com.datasqrl.ai.api.APIQueryExecutor;
import com.datasqrl.ai.api.MockAPIExecutor;
import com.datasqrl.ai.tool.APIFunction;
import com.datasqrl.ai.tool.FunctionUtil;
import graphql.parser.Parser;
import graphql.schema.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GraphQLSchemaConverterTest {

  public static APIQueryExecutor apiExecutor = MockAPIExecutor.of("none");
  public static StandardAPIFunctionFactory fctFactory =
      new StandardAPIFunctionFactory(apiExecutor, Set.of());

  @Test
  public void testNutshop() {
    GraphQLSchemaConverter converter =
        new GraphQLSchemaConverter(
            TestUtil.getResourcesFileAsString("graphql/nutshop-schema.graphqls"),
            GraphQLSchemaConverterConfig.builder()
                .operationFilter(ignorePrefix("internal"))
                .build(),
            new StandardAPIFunctionFactory(apiExecutor, Set.of("customerid")));
    List<APIFunction> functions = converter.convertSchema();
    assertEquals(5, functions.size());
    // Test context key handling
    APIFunction orders =
        functions.stream()
            .filter(f -> f.getFunction().getName().equalsIgnoreCase("orders"))
            .findFirst()
            .get();
    assertTrue(orders.getFunction().getParameters().getProperties().containsKey("customerid"));
    assertFalse(
        orders.getModelFunction().getParameters().getProperties().containsKey("customerid"));
    snapshot(functions, "nutshop");
  }

  @Test
  public void testCreditCard() {
    List<APIFunction> functions = getFunctionsFromPath("graphql/creditcard-rewards.graphqls");
    assertEquals(6, functions.size());
    snapshot(functions, "creditcard-rewards");
  }

  @Test
  public void testLawEnforcement() {
    List<APIFunction> functions = getFunctionsFromPath("graphql/law_enforcement.graphqls");
    assertEquals(7, functions.size());
    snapshot(functions, "law_enforcement");
  }

  @Test
  public void testSensors() {
    GraphQLSchemaConverter converter =
        getConverter(TestUtil.getResourcesFileAsString("graphql/sensors.graphqls"));
    List<APIFunction> functions = converter.convertSchema();
    assertEquals(5, functions.size());
    List<APIFunction> queries =
        converter.convertOperations(
            TestUtil.getResourcesFileAsString("graphql/sensors-aboveTemp.graphql"));
    assertEquals(2, queries.size());
    assertEquals("HighTemps", queries.get(0).getFunction().getName());
    functions.addAll(queries);
    snapshot(functions, "sensors");
  }

  public List<APIFunction> getFunctionsFromPath(String path) {
    return getFunctions(TestUtil.getResourcesFileAsString(path));
  }

  public List<APIFunction> getFunctions(String schemaString) {
    return getConverter(schemaString).convertSchema();
  }

  public GraphQLSchemaConverter getConverter(String schemaString) {
    return new GraphQLSchemaConverter(schemaString, fctFactory);
  }

  @Test
  @Disabled
  public void testSchemaConversion() throws IOException {
    String schemaString =
        Files.readString(
            Path.of(
                "../../../datasqrl-examples/finance-credit-card-chatbot/creditcard-analytics.graphqls"));
    String result = convertToJsonDefault(getFunctions(schemaString));
    System.out.println(result);
  }

  @SneakyThrows
  public static String convertToJsonDefault(List<APIFunction> functions) {
    return FunctionUtil.toJsonString(functions);
  }

  public static void snapshot(List<APIFunction> functions, String testName) {
    for (APIFunction apiFunction : functions) {
      // make sure ALL queries have a good syntax
      var query = apiFunction.getApiQuery().query();
      assertDoesNotThrow(
          () -> {
            Parser.parse(query);
          });
    }
    TestUtil.snapshotTest(
        convertToJsonDefault(functions),
        Path.of("src", "test", "resources", "snapshot", testName + ".json"));
  }

  @Test
  void givenComplexFieldDefinition_whenVisiting_thenGenerateValidQuery() {
    GraphQLSchemaConverter converter =
        new GraphQLSchemaConverter(
            TestUtil.getResourcesFileAsString("graphql/rick_morty-schema.graphqls"),
            GraphQLSchemaConverterConfig.builder()
                .operationFilter(ignorePrefix("internal"))
                .build(),
            new StandardAPIFunctionFactory(apiExecutor, Set.of()));

    List<APIFunction> functions = converter.convertSchema();
    assertEquals(9, functions.size());
    // Test context key handling
    APIFunction episodes =
        functions.stream()
            .filter(f -> f.getFunction().getName().equalsIgnoreCase("episodes"))
            .findFirst()
            .get();
    assertThat(episodes.getFunction().getParameters().getProperties())
        .containsKeys("name", "episode", "page");

    var query = episodes.getApiQuery().query();
    assertDoesNotThrow(
        () -> {
          Parser.parse(query);
        });
    assertFalse(
        episodes.getModelFunction().getParameters().getProperties().containsKey("customerid"));
    snapshot(functions, "rick-morty");
  }
}
