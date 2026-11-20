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
package com.datasqrl.planner.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializer;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorTypeSerializer;
import com.datasqrl.planner.contract.model.OdcsProperty;
import com.datasqrl.planner.util.Documented;
import com.datasqrl.planner.util.Documented.Documentation;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.RawType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OdcsPropertyWalkerTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(OdcsPropertyWalkerTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  private static final NamePath PATH = NamePath.of("col");

  static Stream<Arguments> scalarTypes() {
    return Stream.of(
        Arguments.of(type(SqlTypeName.BOOLEAN), "boolean", null, "BOOLEAN"),
        Arguments.of(type(SqlTypeName.TINYINT), "integer", Map.of("format", "i8"), "TINYINT"),
        Arguments.of(type(SqlTypeName.SMALLINT), "integer", Map.of("format", "i16"), "SMALLINT"),
        Arguments.of(type(SqlTypeName.INTEGER), "integer", Map.of("format", "i32"), "INT"),
        Arguments.of(type(SqlTypeName.BIGINT), "integer", Map.of("format", "i64"), "BIGINT"),
        Arguments.of(
            nullable(TYPES.createSqlType(SqlTypeName.DECIMAL, 10, 2)),
            "number",
            null,
            "DECIMAL(10, 2)"),
        Arguments.of(type(SqlTypeName.FLOAT), "number", Map.of("format", "f32"), "FLOAT"),
        Arguments.of(type(SqlTypeName.DOUBLE), "number", Map.of("format", "f64"), "DOUBLE"),
        Arguments.of(
            nullable(TYPES.createSqlType(SqlTypeName.CHAR, 3)),
            "string",
            Map.of("maxLength", 3),
            "CHAR(3)"),
        Arguments.of(
            nullable(TYPES.createSqlType(SqlTypeName.VARCHAR, 20)),
            "string",
            Map.of("maxLength", 20),
            "VARCHAR(20)"),
        Arguments.of(string(), "string", null, "STRING"),
        Arguments.of(
            nullable(TYPES.createSqlType(SqlTypeName.VARBINARY, 8)),
            "string",
            Map.of("format", "binary"),
            "VARBINARY(8)"),
        Arguments.of(type(SqlTypeName.DATE), "date", null, "DATE"),
        Arguments.of(
            nullable(
                TYPES.createSqlIntervalType(
                    new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO))),
            "string",
            null,
            "INTERVAL SECOND(3)"),
        Arguments.of(jsonType(), "object", null, "JSON"));
  }

  @ParameterizedTest
  @MethodSource("scalarTypes")
  void givenScalarType_whenWalk_thenMapsToLogicalTypeWithOptions(
      RelDataType type, String logicalType, Map<String, Object> options, String physicalType) {
    var property = walk(type, OdcsVersion.V3_2);

    assertThat(property.logicalType()).isEqualTo(logicalType);
    assertThat(property.logicalTypeOptions()).isEqualTo(options);
    assertThat(property.physicalType()).isEqualTo(physicalType);
    assertThat(property.required()).isNull();
    assertThat(property.name()).isNull();
  }

  @Test
  void givenTimeTypes_whenWalk_thenLogicalTypeDependsOnTheVersion() {
    var timestamp = nullable(TYPES.createSqlType(SqlTypeName.TIMESTAMP, 3));
    var timestampLtz = nullable(TYPES.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3));
    var time = type(SqlTypeName.TIME);

    assertThat(walk(timestamp, OdcsVersion.V3_2))
        .extracting(OdcsProperty::logicalType, OdcsProperty::logicalTypeOptions)
        .containsExactly("timestamp", Map.of("timezone", false));
    assertThat(walk(timestampLtz, OdcsVersion.V3_1))
        .extracting(
            OdcsProperty::logicalType, OdcsProperty::logicalTypeOptions, OdcsProperty::physicalType)
        .containsExactly("timestamp", Map.of("timezone", true), "TIMESTAMP_LTZ(3)");
    assertThat(walk(timestampLtz, OdcsVersion.V3_0))
        .extracting(OdcsProperty::logicalType, OdcsProperty::logicalTypeOptions)
        .containsExactly("date", null);
    assertThat(walk(time, OdcsVersion.V3_1).logicalType()).isEqualTo("time");
    assertThat(walk(time, OdcsVersion.V3_0).logicalType()).isEqualTo("string");
  }

  @Test
  void givenMap_whenWalk_thenOnlyTheLatestVersionHasTheMapType() {
    var map =
        nullable(
            TYPES.createMapType(
                TYPES.createTypeWithNullability(string(), false), type(SqlTypeName.INTEGER)));

    var property = walk(map, OdcsVersion.V3_2);

    assertThat(property.logicalType()).isEqualTo("map");
    assertThat(property.physicalType()).isEqualTo("MAP");
    assertThat(property.map().key())
        .extracting(OdcsProperty::logicalType, OdcsProperty::required)
        .containsExactly("string", true);
    assertThat(property.map().value())
        .extracting(OdcsProperty::logicalType, OdcsProperty::required)
        .containsExactly("integer", null);
    assertThat(walk(map, OdcsVersion.V3_1))
        .extracting(OdcsProperty::logicalType, OdcsProperty::map)
        .containsExactly("object", null);
  }

  @Test
  void givenVectorAndUnsupportedType_whenWalk_thenDescribesThemAsFarAsOdcsCan() {
    var vector =
        TYPES.createFieldTypeFromLogicalType(
            new RawType<>(FlinkVectorType.class, new FlinkVectorTypeSerializer()));
    var raw =
        TYPES.createFieldTypeFromLogicalType(
            new RawType<>(String.class, StringSerializer.INSTANCE));

    var vectorProperty = walk(vector, OdcsVersion.V3_2);

    assertThat(vectorProperty)
        .extracting(OdcsProperty::logicalType, OdcsProperty::physicalType)
        .containsExactly("array", "VECTOR");
    assertThat(vectorProperty.items())
        .extracting(OdcsProperty::logicalType, OdcsProperty::logicalTypeOptions)
        .containsExactly("number", Map.of("format", "f64"));
    assertThat(walk(raw, OdcsVersion.V3_2).logicalType()).isNull();
    assertThat(walk(raw, OdcsVersion.V3_2).physicalType()).startsWith("RAW(");
  }

  @Test
  void givenNestedTypes_whenWalk_thenSetsRequiredAndDocsAtEveryLevel() {
    var address =
        TYPES
            .builder()
            .add("city", TYPES.createTypeWithNullability(string(), false))
            .add("zip", string())
            .build();
    var row =
        TYPES
            .builder()
            .add("address", TYPES.createTypeWithNullability(address, false))
            .add(
                "lines",
                nullable(
                    TYPES.createArrayType(TYPES.createTypeWithNullability(address, false), -1)))
            .add("byRegion", nullable(TYPES.createMapType(string(), nullable(address))))
            .build();
    var docs =
        new Documentation(
                null,
                Map.of(
                    NamePath.of("col", "address"), "The address",
                    NamePath.of("col", "address", "city"), "The city",
                    NamePath.of("col", "lines", "zip"), "The zip of a line"),
                Map.of())
            .getColumnLookup();

    var property =
        new OdcsPropertyWalker(OdcsVersion.V3_2, docs)
            .walk(TYPES.createTypeWithNullability(row, false), PATH)
            .orElseThrow();

    assertThat(property)
        .extracting(OdcsProperty::logicalType, OdcsProperty::physicalType, OdcsProperty::required)
        .containsExactly("object", "ROW", true);
    assertThat(property.properties())
        .extracting(
            OdcsProperty::name,
            OdcsProperty::logicalType,
            OdcsProperty::required,
            OdcsProperty::description)
        .containsExactly(
            tuple("address", "object", true, "The address"),
            tuple("lines", "array", null, null),
            tuple("byRegion", "map", null, null));

    var nestedAddress = property.properties().get(0);
    assertThat(nestedAddress.properties())
        .extracting(OdcsProperty::name, OdcsProperty::required, OdcsProperty::description)
        .containsExactly(tuple("city", true, "The city"), tuple("zip", null, null));

    var lineItems = property.properties().get(1).items();
    assertThat(lineItems)
        .extracting(OdcsProperty::name, OdcsProperty::logicalType, OdcsProperty::required)
        .containsExactly(null, "object", true);
    assertThat(lineItems.properties().get(1).description()).isEqualTo("The zip of a line");

    var regionValue = property.properties().get(2).map().value();
    assertThat(regionValue.logicalType()).isEqualTo("object");
    assertThat(regionValue.properties())
        .extracting(OdcsProperty::name)
        .containsExactly("city", "zip");
  }

  private static OdcsProperty walk(RelDataType type, OdcsVersion version) {
    return new OdcsPropertyWalker(version, Documented.NO_LOOKUP).walk(type, PATH).orElseThrow();
  }

  /** The type factory creates NOT NULL types by default. */
  private static RelDataType type(SqlTypeName typeName) {
    return nullable(TYPES.createSqlType(typeName));
  }

  private static RelDataType string() {
    return nullable(TYPES.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE));
  }

  private static RelDataType nullable(RelDataType type) {
    return TYPES.createTypeWithNullability(type, true);
  }

  private static RelDataType jsonType() {
    return nullable(
        TYPES.createFieldTypeFromLogicalType(
            new RawType<>(FlinkJsonType.class, new FlinkJsonTypeSerializer())));
  }
}
