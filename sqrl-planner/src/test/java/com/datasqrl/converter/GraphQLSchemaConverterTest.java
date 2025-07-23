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
package com.datasqrl.converter;

import static com.datasqrl.graphql.converter.GraphQLSchemaConverterConfig.ignorePrefix;
import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLID;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.datasqrl.graphql.converter.GraphQLSchemaConverter;
import com.datasqrl.graphql.converter.GraphQLSchemaConverterConfig;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.graphql.server.operation.ApiOperation;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import graphql.parser.Parser;
import graphql.schema.GraphQLScalarType;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class GraphQLSchemaConverterTest {

  GraphQLSchemaConverter underTest = new GraphQLSchemaConverter();

  @Test
  public void testNutshop() {
    var schema = underTest.getSchema(getTestSchema("nutshop-schema.graphqls"));
    var config =
        GraphQLSchemaConverterConfig.builder()
            .addPrefix(false)
            .operationFilter(ignorePrefix("internal"))
            .build();

    List<ApiOperation> functions = underTest.convertSchema(config, schema);
    assertThat(functions).hasSize(5);
    // Test context key handling
    ApiOperation orders =
        functions.stream()
            .filter(f -> f.getFunction().getName().equalsIgnoreCase("orders"))
            .findFirst()
            .get();
    assertThat(orders.getFunction().getParameters().getProperties()).containsKey("customerid");
    snapshot(functions, "nutshop");
  }

  @Test
  public void testCreditCard() {
    List<ApiOperation> functions = getFunctionsFromPath("creditcard-rewards.graphqls");
    assertThat(functions).hasSize(6);
    snapshot(functions, "creditcard-rewards");
  }

  @Test
  public void testLawEnforcement() {
    List<ApiOperation> functions = getFunctionsFromPath("law_enforcement.graphqls");
    assertThat(functions).hasSize(7);
    snapshot(functions, "law_enforcement");
  }

  @Test
  public void testSensors() {
    var schema = underTest.getSchema(getTestSchema("sensors.graphqls"));
    var config = GraphQLSchemaConverterConfig.DEFAULT;

    List<ApiOperation> functions = underTest.convertSchema(config, schema);
    assertThat(functions).hasSize(5);
    List<ApiOperation> queries =
        underTest.convertOperations(getTestSchema("sensors-aboveTemp.graphql"), config, schema);
    assertThat(queries).hasSize(2);
    assertThat(queries.get(0).getFunction().getName()).isEqualTo("HighTemps");
    functions.addAll(queries);
    snapshot(functions, "sensors");
  }

  public List<ApiOperation> getFunctionsFromPath(String path) {
    return getFunctions(getTestSchema(path));
  }

  public List<ApiOperation> getFunctions(String schemaString) {
    return underTest.convertSchema(
        GraphQLSchemaConverterConfig.DEFAULT, underTest.getSchema(schemaString));
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
  public static String convertToJsonDefault(List<ApiOperation> functions) {
    return toJsonString(functions);
  }

  public void snapshot(List<ApiOperation> functions, String testName) {
    for (ApiOperation apiOperation : functions) {
      // make sure ALL queries have a good syntax
      var query = apiOperation.getApiQuery().query();
      assertThatCode(() -> Parser.parse(query)).doesNotThrowAnyException();
    }
    var snapshot = SnapshotTest.Snapshot.of(getClass(), testName);
    snapshot.addContent(convertToJsonDefault(functions));
    snapshot.createOrValidate();
  }

  @Test
  void testRickandMorty() {
    var schema = underTest.getSchema(getTestSchema("rick_morty-schema.graphqls"));
    var config =
        GraphQLSchemaConverterConfig.builder().operationFilter(ignorePrefix("internal")).build();

    List<ApiOperation> functions = underTest.convertSchema(config, schema);
    assertThat(functions).hasSize(9);
    // Test context key handling
    ApiOperation episodes =
        functions.stream()
            .filter(f -> f.getFunction().getName().equalsIgnoreCase("getepisodes"))
            .findFirst()
            .get();
    assertThat(episodes.getFunction().getParameters().getProperties())
        .containsKeys("name", "episode", "page");

    var query = episodes.getApiQuery().query();
    assertThatCode(() -> Parser.parse(query)).doesNotThrowAnyException();
    assertThat(episodes.getFunction().getParameters().getProperties())
        .doesNotContainKey("customerid");
    snapshot(functions, "rick-morty");
  }

  @ParameterizedTest
  @MethodSource("scalarTypeToJsonTypeProvider")
  void givenScalarType_whenConvertToJsonType_thenReturnsExpectedJsonType(
      GraphQLScalarType scalarType, String expectedJsonType) {
    // When
    var result = underTest.convertScalarTypeToJsonType(scalarType);

    // Then
    assertThat(result).isEqualTo(expectedJsonType);
  }

  static Stream<Arguments> scalarTypeToJsonTypeProvider() {
    return Stream.of(
        Arguments.of(GraphQLInt, "integer"),
        Arguments.of(CustomScalars.LONG, "integer"),
        Arguments.of(GraphQLFloat, "number"),
        Arguments.of(GraphQLString, "string"),
        Arguments.of(GraphQLBoolean, "boolean"),
        Arguments.of(GraphQLID, "string"));
  }

  @SneakyThrows
  private static String getTestSchema(String schemaFile) {
    return FileUtil.readResource("graphql/converter/" + schemaFile);
  }

  public static String toJsonString(List<ApiOperation> tools) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper
        .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
        .enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.valueToTree(tools));
  }
}
