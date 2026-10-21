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

import static com.datasqrl.server.converter.GraphQLSchemaConverterConfig.ignorePrefix;
import static graphql.Scalars.GraphQLBoolean;
import static graphql.Scalars.GraphQLFloat;
import static graphql.Scalars.GraphQLID;
import static graphql.Scalars.GraphQLInt;
import static graphql.Scalars.GraphQLString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.datasqrl.server.converter.GraphQLSchemaConverter;
import com.datasqrl.server.converter.GraphQLSchemaConverterConfig;
import com.datasqrl.server.graphql.CustomScalars;
import com.datasqrl.server.operation.ApiOperation;
import com.datasqrl.server.operation.McpMethodType;
import com.datasqrl.server.operation.RestMethodType;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import graphql.parser.Parser;
import graphql.schema.GraphQLScalarType;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import lombok.SneakyThrows;
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

  @Test
  @SneakyThrows
  void givenJsonArgument_whenConvertSchema_thenCreatesUnconstrainedJsonSchema() {
    var schema =
        underTest.getSchema(
            """
            scalar JSON
            type Query {
              lookup(payload: JSON): String
            }
            """);

    var functions = underTest.convertSchema(GraphQLSchemaConverterConfig.DEFAULT, schema);
    var payload = functions.get(0).getFunction().getParameters().getProperties().get("payload");
    var payloadSchema =
        new ObjectMapper()
            .readTree(toJsonString(functions))
            .at("/0/function/parameters/properties/payload");

    assertThat(payload.getType()).isNull();
    assertThat(payloadSchema.has("type")).isFalse();
  }

  @Test
  void givenOperationWithNestedResult_whenConvertOperations_thenCreatesResultDefinition() {
    var schema =
        underTest.getSchema(
            """
            scalar DateTime
            type Query {
              reading: Reading!
            }
            type Reading {
              id: Int!
              observedAt: DateTime
            }
            """);

    var operations =
        underTest.convertOperations(
            "query Reading { reading { id observedAt } }",
            GraphQLSchemaConverterConfig.DEFAULT,
            schema);

    var result = operations.get(0).getResultDefinition();
    assertThat(result.getType()).isEqualTo("object");
    assertThat(result.getProperties().get("id").getType()).isEqualTo("integer");
    assertThat(result.getProperties().get("observedAt").getFormat()).isEqualTo("date-time");
  }

  @Test
  void givenQueryWithoutArguments_whenConvertSchema_thenOmitsVariableDefinitions() {
    var schema =
        underTest.getSchema(
            """
            type Query {
              counter: Counter!
            }
            type Counter {
              numEvents: Int!
            }
            """);

    var operations = underTest.convertSchema(GraphQLSchemaConverterConfig.DEFAULT, schema);

    assertThat(operations).hasSize(1);
    var query = operations.get(0).getApiQuery().query();
    assertThat(query).startsWith("query counter {");
    assertThatCode(() -> Parser.parse(query)).doesNotThrowAnyException();
    assertThat(operations.get(0).getResultDefinition().getProperties()).containsKey("numEvents");
  }

  @Test
  void givenMutationWithNestedInput_whenConvertSchema_thenExposesMcpToolWithNestedSchema() {
    var functions = getFunctionsFromPath("nested-mutation.graphqls");

    assertThat(functions)
        .extracting(ApiOperation::getName)
        .containsExactlyInAnyOrder("GetOrders", "AddAddOrder", "AddAddReadings");
    assertThat(functions)
        .allSatisfy(op -> assertThat(op.getMcpMethod()).isEqualTo(McpMethodType.TOOL));

    var addOrder = getOperation(functions, "AddAddOrder");
    assertThat(addOrder.getRestMethod()).isEqualTo(RestMethodType.POST);
    var params = addOrder.getFunction().getParameters();
    assertThat(params.getProperties())
        .containsOnlyKeys("id", "customerid", "time", "shipping", "entries", "tags", "metadata");
    assertThat(params.getRequired())
        .containsExactlyInAnyOrder("id", "customerid", "time", "entries");

    var shipping = params.getProperties().get("shipping");
    assertThat(shipping.getType()).isEqualTo("object");
    assertThat(shipping.getDescription()).isEqualTo("Where the order is shipped to");
    assertThat(shipping.getRequired()).containsExactlyInAnyOrder("name", "address");
    var address = shipping.getProperties().get("address");
    assertThat(address.getType()).isEqualTo("object");
    assertThat(address.getProperties()).containsOnlyKeys("street", "city", "zip");
    assertThat(address.getRequired()).containsExactlyInAnyOrder("street", "city");

    var entries = params.getProperties().get("entries");
    assertThat(entries.getType()).isEqualTo("array");
    assertThat(entries.getItems().getType()).isEqualTo("object");
    assertThat(entries.getItems().getProperties().get("quantity").getType()).isEqualTo("integer");
    assertThat(entries.getItems().getProperties().get("unit_price").getType()).isEqualTo("number");
    assertThat(entries.getItems().getRequired())
        .containsExactlyInAnyOrder("productid", "quantity", "unit_price");

    var addReadings = getOperation(functions, "AddAddReadings");
    var event = addReadings.getFunction().getParameters().getProperties().get("event");
    assertThat(event.getType()).isEqualTo("array");
    assertThat(event.getItems().getProperties()).containsOnlyKeys("sensorid", "temperature");
    assertThat(addReadings.getFunction().getParameters().getRequired()).containsExactly("event");

    snapshot(functions, "nested-mutation");
  }

  @Test
  void givenNestedInputWithApiDirective_whenConvertSchema_thenDirectiveOverridesMcpDefault() {
    var schema =
        underTest.getSchema(
            """
            enum _McpMethodType { NONE TOOL RESOURCE }
            enum _RestMethodType { NONE GET POST }
            directive @api(mcp: _McpMethodType, rest: _RestMethodType, uri: String) on QUERY | MUTATION | FIELD_DEFINITION

            type Query {
              ping: String
            }
            type Mutation {
              AddHidden(event: OrderInput!): String @api(mcp: NONE)
              AddVisible(event: OrderInput!): String
            }
            input OrderInput {
              id: Int!
              shipping: ShippingInput
            }
            input ShippingInput {
              city: String
            }
            """);

    var functions = underTest.convertSchema(GraphQLSchemaConverterConfig.DEFAULT, schema);

    assertThat(getOperation(functions, "AddAddHidden").getMcpMethod())
        .isEqualTo(McpMethodType.NONE);
    assertThat(getOperation(functions, "AddAddHidden").getRestMethod())
        .isEqualTo(RestMethodType.POST);
    assertThat(getOperation(functions, "AddAddVisible").getMcpMethod())
        .isEqualTo(McpMethodType.TOOL);
  }

  @Test
  void givenOperationWithInputObjectVariable_whenConvertOperations_thenExposesMcpTool() {
    var schema = underTest.getSchema(getTestSchema("nested-mutation.graphqls"));

    var operations =
        underTest.convertOperations(
            """
            mutation PlaceOrder($order: OrderInput!) {
              AddOrder(event: $order) { id }
            }
            mutation PlaceHiddenOrder($order: OrderInput!) @api(mcp: NONE) {
              AddOrder(event: $order) { id }
            }
            """,
            GraphQLSchemaConverterConfig.DEFAULT,
            schema);

    var placeOrder = getOperation(operations, "PlaceOrder");
    assertThat(placeOrder.getMcpMethod()).isEqualTo(McpMethodType.TOOL);
    var order = placeOrder.getFunction().getParameters().getProperties().get("order");
    assertThat(order.getType()).isEqualTo("object");
    assertThat(order.getProperties().get("shipping").getProperties().get("address").getType())
        .isEqualTo("object");
    assertThat(order.getProperties().get("entries").getItems().getType()).isEqualTo("object");
    assertThat(placeOrder.getFunction().getParameters().getRequired()).containsExactly("order");

    assertThat(getOperation(operations, "PlaceHiddenOrder").getMcpMethod())
        .isEqualTo(McpMethodType.NONE);
  }

  private static ApiOperation getOperation(List<ApiOperation> operations, String name) {
    return operations.stream()
        .filter(op -> op.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Operation not found: " + name));
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
        Arguments.of(GraphQLID, "string"),
        Arguments.of(CustomScalars.JSON, null));
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
