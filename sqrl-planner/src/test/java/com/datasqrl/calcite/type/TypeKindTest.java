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
package com.datasqrl.calcite.type;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializer;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorTypeSerializer;
import java.util.List;
import java.util.stream.Stream;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.types.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TypeKindTest {

  private static final FlinkTypeFactory TYPES =
      new FlinkTypeFactory(TypeKindTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

  static Stream<Arguments> types() {
    var integer = type(SqlTypeName.INTEGER);
    return Stream.of(
        Arguments.of(type(SqlTypeName.BOOLEAN), TypeKind.BOOLEAN),
        Arguments.of(type(SqlTypeName.TINYINT), TypeKind.TINYINT),
        Arguments.of(type(SqlTypeName.SMALLINT), TypeKind.SMALLINT),
        Arguments.of(integer, TypeKind.INTEGER),
        Arguments.of(type(SqlTypeName.BIGINT), TypeKind.BIGINT),
        Arguments.of(TYPES.createSqlType(SqlTypeName.DECIMAL, 10, 2), TypeKind.DECIMAL),
        Arguments.of(type(SqlTypeName.FLOAT), TypeKind.FLOAT),
        Arguments.of(type(SqlTypeName.REAL), TypeKind.FLOAT),
        Arguments.of(type(SqlTypeName.DOUBLE), TypeKind.DOUBLE),
        Arguments.of(type(SqlTypeName.DATE), TypeKind.DATE),
        Arguments.of(type(SqlTypeName.TIME), TypeKind.TIME),
        Arguments.of(type(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE), TypeKind.TIME_LTZ),
        Arguments.of(type(SqlTypeName.TIMESTAMP), TypeKind.TIMESTAMP),
        Arguments.of(type(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE), TypeKind.TIMESTAMP_LTZ),
        Arguments.of(
            TYPES.createSqlIntervalType(
                new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)),
            TypeKind.INTERVAL),
        Arguments.of(
            TYPES.createSqlIntervalType(
                new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO)),
            TypeKind.INTERVAL),
        Arguments.of(TYPES.createSqlType(SqlTypeName.CHAR, 3), TypeKind.STRING),
        Arguments.of(type(SqlTypeName.VARCHAR), TypeKind.STRING),
        Arguments.of(type(SqlTypeName.BINARY), TypeKind.BINARY),
        Arguments.of(type(SqlTypeName.VARBINARY), TypeKind.BINARY),
        Arguments.of(
            raw(new RawType<>(FlinkJsonType.class, new FlinkJsonTypeSerializer())), TypeKind.JSON),
        Arguments.of(
            raw(new RawType<>(FlinkVectorType.class, new FlinkVectorTypeSerializer())),
            TypeKind.VECTOR),
        Arguments.of(
            raw(new RawType<>(String.class, StringSerializer.INSTANCE)), TypeKind.UNSUPPORTED),
        Arguments.of(TYPES.builder().add("id", integer).build(), TypeKind.ROW),
        Arguments.of(
            TYPES.createFieldTypeFromLogicalType(
                StructuredType.newBuilder(Row.class)
                    .attributes(List.of(new StructuredAttribute("id", new IntType())))
                    .build()),
            TypeKind.ROW),
        Arguments.of(TYPES.createArrayType(integer, -1), TypeKind.ARRAY),
        Arguments.of(TYPES.createMultisetType(integer, -1), TypeKind.ARRAY),
        Arguments.of(TYPES.createMapType(type(SqlTypeName.VARCHAR), integer), TypeKind.MAP),
        Arguments.of(type(SqlTypeName.NULL), TypeKind.UNSUPPORTED),
        Arguments.of(type(SqlTypeName.ANY), TypeKind.UNSUPPORTED),
        Arguments.of(type(SqlTypeName.SYMBOL), TypeKind.UNSUPPORTED));
  }

  @ParameterizedTest
  @MethodSource("types")
  void givenType_whenOf_thenClassifiesIt(RelDataType type, TypeKind expected) {
    assertThat(TypeKind.of(type)).isEqualTo(expected);
  }

  private static RelDataType type(SqlTypeName typeName) {
    return TYPES.createSqlType(typeName);
  }

  private static RelDataType raw(RawType<?> rawType) {
    return TYPES.createFieldTypeFromLogicalType(rawType);
  }
}
