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
package com.datasqrl.server.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializer;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorTypeSerializer;
import com.datasqrl.planner.util.Documented;
import com.datasqrl.planner.util.Documented.Documentation;
import com.datasqrl.planner.util.Documented.Lookup;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeUtil;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.RawType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Pins the mapping from SQL types to GraphQL types. */
class GraphqlTypeWalkerTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(GraphqlTypeWalkerTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final NamePath PATH = NamePath.of("Orders");

  static Stream<Arguments> scalarTypes() {
    return Stream.of(
        Arguments.of(type(SqlTypeName.BOOLEAN), false, "Boolean"),
        Arguments.of(type(SqlTypeName.TINYINT), false, "Int"),
        Arguments.of(type(SqlTypeName.SMALLINT), false, "Int"),
        Arguments.of(type(SqlTypeName.INTEGER), false, "Int"),
        Arguments.of(type(SqlTypeName.BIGINT), false, "Float"),
        Arguments.of(type(SqlTypeName.BIGINT), true, "Long"),
        Arguments.of(nullable(TYPES.createSqlType(SqlTypeName.DECIMAL, 10, 2)), false, "Float"),
        Arguments.of(type(SqlTypeName.FLOAT), false, "Float"),
        Arguments.of(type(SqlTypeName.REAL), false, "Float"),
        Arguments.of(type(SqlTypeName.DOUBLE), true, "Float"),
        Arguments.of(type(SqlTypeName.DATE), false, "Date"),
        Arguments.of(type(SqlTypeName.TIME), false, "LocalTime"),
        Arguments.of(type(SqlTypeName.TIMESTAMP), false, "DateTime"),
        Arguments.of(type(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE), false, "DateTime"),
        Arguments.of(nullable(TYPES.createSqlType(SqlTypeName.CHAR, 3)), false, "String"),
        Arguments.of(type(SqlTypeName.VARCHAR), false, "String"),
        Arguments.of(
            nullable(
                TYPES.createSqlIntervalType(
                    new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))),
            false,
            "String"),
        Arguments.of(
            nullable(TYPES.createMapType(type(SqlTypeName.VARCHAR), type(SqlTypeName.INTEGER))),
            false,
            "JSON"),
        Arguments.of(jsonType(), false, "JSON"));
  }

  @ParameterizedTest
  @MethodSource("scalarTypes")
  void givenScalarType_whenGetGraphQLType_thenMapsToGraphQLScalar(
      RelDataType type, boolean extendedScalarTypes, String expected) {
    var gqlType = outputType(type, extendedScalarTypes, Documented.NO_LOOKUP);

    assertThat(gqlType).map(GraphQLTypeUtil::simplePrint).contains(expected);
  }

  @Test
  void givenUnsupportedTypes_whenGetGraphQLType_thenReturnsEmpty() {
    assertThat(outputType(type(SqlTypeName.BINARY), false, Documented.NO_LOOKUP)).isEmpty();
    assertThat(outputType(type(SqlTypeName.VARBINARY), false, Documented.NO_LOOKUP)).isEmpty();
    assertThat(outputType(vectorType(), false, Documented.NO_LOOKUP)).isEmpty();
  }

  @Test
  void givenNotNullType_whenGetGraphQLOutputType_thenWrapsInNonNull() {
    var notNull = TYPES.createTypeWithNullability(type(SqlTypeName.INTEGER), false);

    assertThat(outputType(notNull, false, Documented.NO_LOOKUP))
        .map(GraphQLTypeUtil::simplePrint)
        .contains("Int!");
  }

  @Test
  void givenArrayOfNotNullElements_whenGetGraphQLOutputType_thenElementsAreNotWrapped() {
    var element = TYPES.createTypeWithNullability(type(SqlTypeName.INTEGER), false);
    var array = TYPES.createTypeWithNullability(TYPES.createArrayType(element, -1), false);

    assertThat(outputType(array, false, Documented.NO_LOOKUP))
        .map(GraphQLTypeUtil::simplePrint)
        .contains("[Int]!");
  }

  @Test
  void givenRowWithNestedRow_whenGetGraphQLOutputType_thenBuildsObjectTypesWithDocs() {
    var address =
        TYPES
            .builder()
            .add("city", TYPES.createTypeWithNullability(type(SqlTypeName.VARCHAR), false))
            .add("_internal", type(SqlTypeName.VARCHAR))
            .build();
    var row =
        TYPES
            .builder()
            .add("id", TYPES.createTypeWithNullability(type(SqlTypeName.BIGINT), false))
            .add("payload", type(SqlTypeName.VARBINARY))
            .add("_hidden", type(SqlTypeName.INTEGER))
            .add("address", TYPES.createTypeWithNullability(address, false))
            .add("lines", nullable(TYPES.createArrayType(address, -1)))
            .build();
    var docs =
        new Documentation(
                null,
                Map.of(
                    NamePath.of("id"), "The order id",
                    NamePath.of("address"), "The address",
                    NamePath.of("address", "city"), "The city",
                    NamePath.of("lines", "city"), "The city of the line"),
                Map.of())
            .getColumnLookup();

    var gqlType = outputType(TYPES.createTypeWithNullability(row, false), true, docs).orElseThrow();

    var object = (GraphQLObjectType) GraphQLTypeUtil.unwrapNonNull(gqlType);
    assertThat(gqlType).isInstanceOf(GraphQLNonNull.class);
    assertThat(object.getName()).isEqualTo("OrdersOutput");
    assertThat(object.getFieldDefinitions())
        .extracting(
            field -> field.getName(),
            field -> GraphQLTypeUtil.simplePrint(field.getType()),
            field -> field.getDescription())
        .containsExactly(
            tuple("id", "Long!", "The order id"),
            tuple("address", "Orders_addressOutput!", "The address"),
            tuple("lines", "[Orders_linesOutput]", null));

    var nested =
        (GraphQLObjectType)
            GraphQLTypeUtil.unwrapNonNull(object.getFieldDefinition("address").getType());
    assertThat(nested.getFieldDefinitions())
        .extracting(
            field -> field.getName(),
            field -> GraphQLTypeUtil.simplePrint(field.getType()),
            field -> field.getDescription())
        .containsExactly(tuple("city", "String!", "The city"));

    var line =
        (GraphQLObjectType) GraphQLTypeUtil.unwrapAll(object.getFieldDefinition("lines").getType());
    assertThat(line.getFieldDefinition("city").getDescription()).isEqualTo("The city of the line");
  }

  @Test
  void givenRow_whenGetGraphQLInputType_thenBuildsInputObjectType() {
    var row =
        TYPES
            .builder()
            .add("id", TYPES.createTypeWithNullability(type(SqlTypeName.INTEGER), false))
            .add("name", type(SqlTypeName.VARCHAR))
            .build();
    var docs =
        new Documentation(null, Map.of(NamePath.of("name"), "The name"), Map.of())
            .getColumnLookup();

    var gqlType =
        GraphqlSchemaUtil.getGraphQLInputType(row, PATH, false, docs).map(GraphQLType.class::cast);

    var input = (GraphQLInputObjectType) GraphQLTypeUtil.unwrapNonNull(gqlType.orElseThrow());
    assertThat(input.getName()).isEqualTo("OrdersInput");
    assertThat(input.getFieldDefinitions())
        .extracting(
            field -> field.getName(),
            field -> GraphQLTypeUtil.simplePrint(field.getType()),
            field -> field.getDescription())
        .containsExactly(tuple("id", "Int!", null), tuple("name", "String", "The name"));
  }

  private static Optional<GraphQLType> outputType(
      RelDataType type, boolean extendedScalarTypes, Lookup docs) {
    return GraphqlSchemaUtil.getGraphQLOutputType(type, PATH, extendedScalarTypes, docs)
        .map(GraphQLType.class::cast);
  }

  /** The type factory creates NOT NULL types by default. */
  private static RelDataType type(SqlTypeName typeName) {
    return nullable(TYPES.createSqlType(typeName));
  }

  private static RelDataType nullable(RelDataType type) {
    return TYPES.createTypeWithNullability(type, true);
  }

  private static RelDataType jsonType() {
    return nullable(
        TYPES.createFieldTypeFromLogicalType(
            new RawType<>(FlinkJsonType.class, new FlinkJsonTypeSerializer())));
  }

  private static RelDataType vectorType() {
    return TYPES.createFieldTypeFromLogicalType(
        new RawType<>(FlinkVectorType.class, new FlinkVectorTypeSerializer()));
  }
}
