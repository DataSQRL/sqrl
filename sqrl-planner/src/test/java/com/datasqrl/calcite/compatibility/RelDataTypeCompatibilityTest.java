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
package com.datasqrl.calcite.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RelDataTypeCompatibilityTest {

  private RelDataTypeCompatibility compatibility;
  private RelDataTypeFactory typeFactory;

  @BeforeEach
  void setUp() {
    compatibility = new RelDataTypeCompatibility();
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  @Nested
  class NullabilityTests {

    @Test
    void givenBothNonNullable_whenCheckCompatibility_thenReturnsTrue() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

      assertThat(compatibility.isBackwardsCompatible(intType, intType)).isTrue();
    }

    @Test
    void givenBothNullable_whenCheckCompatibility_thenReturnsTrue() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var nullableInt = typeFactory.createTypeWithNullability(intType, true);

      assertThat(compatibility.isBackwardsCompatible(nullableInt, nullableInt)).isTrue();
    }

    @Test
    void givenReaderNullableWriterNonNullable_whenCheckCompatibility_thenReturnsTrue() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var nullableInt = typeFactory.createTypeWithNullability(intType, true);

      assertThat(compatibility.isBackwardsCompatible(nullableInt, intType)).isTrue();
    }

    @Test
    void givenReaderNonNullableWriterNullable_whenCheckCompatibility_thenReturnsFalse() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var nullableInt = typeFactory.createTypeWithNullability(intType, true);

      assertThat(compatibility.isBackwardsCompatible(intType, nullableInt)).isFalse();
    }
  }

  @Nested
  class PrimitiveTypeTests {

    @Test
    void givenSameTypes_whenCheckCompatibility_thenReturnsTrue() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
      var booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

      assertThat(compatibility.isBackwardsCompatible(intType, intType)).isTrue();
      assertThat(compatibility.isBackwardsCompatible(varcharType, varcharType)).isTrue();
      assertThat(compatibility.isBackwardsCompatible(booleanType, booleanType)).isTrue();
    }

    @Test
    void givenWiderIntegerType_whenCheckCompatibility_thenReturnsTrue() {
      var smallintType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);

      // SMALLINT -> INT is widening (compatible)
      assertThat(compatibility.isBackwardsCompatible(intType, smallintType)).isTrue();
      // INT -> BIGINT is widening (compatible)
      assertThat(compatibility.isBackwardsCompatible(bigintType, intType)).isTrue();
      // SMALLINT -> BIGINT is widening (compatible)
      assertThat(compatibility.isBackwardsCompatible(bigintType, smallintType)).isTrue();
    }

    @Test
    void givenNarrowerIntegerType_whenCheckCompatibility_thenReturnsFalse() {
      var smallintType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var bigintType = typeFactory.createSqlType(SqlTypeName.BIGINT);

      // Narrowing is NOT allowed for backwards compatibility - could cause overflow
      // SMALLINT reader cannot safely read INT data (values may exceed SMALLINT range)
      assertThat(compatibility.isBackwardsCompatible(smallintType, intType)).isFalse();
      // INT reader cannot safely read BIGINT data (values may exceed INT range)
      assertThat(compatibility.isBackwardsCompatible(intType, bigintType)).isFalse();
    }

    @Test
    void givenFloatToDouble_whenCheckCompatibility_thenReturnsTrue() {
      var floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
      var doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

      assertThat(compatibility.isBackwardsCompatible(doubleType, floatType)).isTrue();
    }

    @Test
    void givenVarcharWithDifferentPrecision_whenCheckCompatibility_thenHandlesWidthCorrectly() {
      var varchar100 = typeFactory.createSqlType(SqlTypeName.VARCHAR, 100);
      var varchar255 = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);

      // Larger varchar can read from smaller varchar (widening - safe)
      assertThat(compatibility.isBackwardsCompatible(varchar255, varchar100)).isTrue();
      // Smaller varchar cannot read from larger (narrowing - could truncate)
      assertThat(compatibility.isBackwardsCompatible(varchar100, varchar255)).isFalse();
    }

    @Test
    void givenIncompatibleTypes_whenCheckCompatibility_thenReturnsFalse() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

      // INT and BOOLEAN are not compatible
      assertThat(compatibility.isBackwardsCompatible(intType, booleanType)).isFalse();
      assertThat(compatibility.isBackwardsCompatible(booleanType, intType)).isFalse();
    }

    @Test
    void givenDecimalTypes_whenCheckCompatibility_thenReturnsTrue() {
      var decimal10_2 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);
      var decimal15_2 = typeFactory.createSqlType(SqlTypeName.DECIMAL, 15, 2);

      assertThat(compatibility.isBackwardsCompatible(decimal15_2, decimal10_2)).isTrue();
    }
  }

  @Nested
  class StructTests {

    @Test
    void givenIdenticalStructs_whenCheckCompatibility_thenReturnsTrue() {
      var struct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .build();

      assertThat(compatibility.isBackwardsCompatible(struct, struct)).isTrue();
    }

    @Test
    void givenReaderHasNewNullableField_whenCheckCompatibility_thenReturnsTrue() {
      var writerStruct =
          typeFactory.builder().add("id", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();

      var nullableVarchar =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true);
      var readerStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("name", nullableVarchar)
              .build();

      assertThat(compatibility.isBackwardsCompatible(readerStruct, writerStruct)).isTrue();
    }

    @Test
    void givenReaderHasNewNonNullableField_whenCheckCompatibility_thenReturnsFalse() {
      var writerStruct =
          typeFactory.builder().add("id", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();

      var readerStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .build();

      assertThat(compatibility.isBackwardsCompatible(readerStruct, writerStruct)).isFalse();
    }

    @Test
    void givenWriterHasExtraField_whenCheckCompatibility_thenReturnsTrue() {
      var writerStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .add("extra", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
              .build();

      var readerStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .build();

      // Reader ignores extra fields from writer
      assertThat(compatibility.isBackwardsCompatible(readerStruct, writerStruct)).isTrue();
    }

    @Test
    void givenStructFieldTypeIncompatible_whenCheckCompatibility_thenReturnsFalse() {
      var writerStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .build();

      var readerStruct =
          typeFactory.builder().add("id", typeFactory.createSqlType(SqlTypeName.BOOLEAN)).build();

      assertThat(compatibility.isBackwardsCompatible(readerStruct, writerStruct)).isFalse();
    }

    @Test
    void givenStructFieldTypeWidened_whenCheckCompatibility_thenReturnsTrue() {
      var writerStruct =
          typeFactory
              .builder()
              .add("count", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .build();

      var readerStruct =
          typeFactory.builder().add("count", typeFactory.createSqlType(SqlTypeName.BIGINT)).build();

      assertThat(compatibility.isBackwardsCompatible(readerStruct, writerStruct)).isTrue();
    }
  }

  @Nested
  class NestedStructTests {

    @Test
    void givenNestedStructsIdentical_whenCheckCompatibility_thenReturnsTrue() {
      var innerStruct =
          typeFactory
              .builder()
              .add("street", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .add("city", typeFactory.createSqlType(SqlTypeName.VARCHAR, 100))
              .build();

      var outerStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("address", innerStruct)
              .build();

      assertThat(compatibility.isBackwardsCompatible(outerStruct, outerStruct)).isTrue();
    }

    @Test
    void givenNestedStructReaderHasNewNullableField_whenCheckCompatibility_thenReturnsTrue() {
      var writerInnerStruct =
          typeFactory
              .builder()
              .add("street", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .build();

      var writerOuterStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("address", writerInnerStruct)
              .build();

      var nullableCity =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 100), true);
      var readerInnerStruct =
          typeFactory
              .builder()
              .add("street", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .add("city", nullableCity)
              .build();

      var readerOuterStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("address", readerInnerStruct)
              .build();

      assertThat(compatibility.isBackwardsCompatible(readerOuterStruct, writerOuterStruct))
          .isTrue();
    }

    @Test
    void givenNestedStructReaderHasNewNonNullableField_whenCheckCompatibility_thenReturnsFalse() {
      var writerInnerStruct =
          typeFactory
              .builder()
              .add("street", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .build();

      var writerOuterStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("address", writerInnerStruct)
              .build();

      var readerInnerStruct =
          typeFactory
              .builder()
              .add("street", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .add("city", typeFactory.createSqlType(SqlTypeName.VARCHAR, 100))
              .build();

      var readerOuterStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("address", readerInnerStruct)
              .build();

      assertThat(compatibility.isBackwardsCompatible(readerOuterStruct, writerOuterStruct))
          .isFalse();
    }

    @Test
    void givenDeeplyNestedStructs_whenCheckCompatibility_thenReturnsTrue() {
      var level3 =
          typeFactory
              .builder()
              .add("value", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .build();

      var level2 = typeFactory.builder().add("nested", level3).build();

      var level1 = typeFactory.builder().add("data", level2).build();

      assertThat(compatibility.isBackwardsCompatible(level1, level1)).isTrue();
    }
  }

  @Nested
  class ArrayTests {

    @Test
    void givenIdenticalArrays_whenCheckCompatibility_thenReturnsTrue() {
      var intArray =
          typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);

      assertThat(compatibility.isBackwardsCompatible(intArray, intArray)).isTrue();
    }

    @Test
    void givenArrayWithWidenedElementType_whenCheckCompatibility_thenReturnsTrue() {
      var intArray =
          typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
      var bigintArray =
          typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BIGINT), -1);

      assertThat(compatibility.isBackwardsCompatible(bigintArray, intArray)).isTrue();
    }

    @Test
    void givenArrayWithIncompatibleElementType_whenCheckCompatibility_thenReturnsFalse() {
      var intArray =
          typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
      var booleanArray =
          typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), -1);

      assertThat(compatibility.isBackwardsCompatible(intArray, booleanArray)).isFalse();
    }

    @Test
    void givenArrayOfStructs_whenCheckCompatibility_thenReturnsTrue() {
      var struct =
          typeFactory.builder().add("id", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();

      var structArray = typeFactory.createArrayType(struct, -1);

      assertThat(compatibility.isBackwardsCompatible(structArray, structArray)).isTrue();
    }

    @Test
    void givenArrayOfStructsWithNewNullableField_whenCheckCompatibility_thenReturnsTrue() {
      var writerStruct =
          typeFactory.builder().add("id", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();

      var nullableName =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true);
      var readerStruct =
          typeFactory
              .builder()
              .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
              .add("name", nullableName)
              .build();

      var writerArray = typeFactory.createArrayType(writerStruct, -1);
      var readerArray = typeFactory.createArrayType(readerStruct, -1);

      assertThat(compatibility.isBackwardsCompatible(readerArray, writerArray)).isTrue();
    }

    @Test
    void givenNestedArrays_whenCheckCompatibility_thenReturnsTrue() {
      var innerArray =
          typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
      var outerArray = typeFactory.createArrayType(innerArray, -1);

      assertThat(compatibility.isBackwardsCompatible(outerArray, outerArray)).isTrue();
    }

    @Test
    void givenArrayElementNullabilityMismatch_whenCheckCompatibility_thenReturnsFalse() {
      var nonNullableInt = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var nullableInt = typeFactory.createTypeWithNullability(nonNullableInt, true);

      var nonNullableArray = typeFactory.createArrayType(nonNullableInt, -1);
      var nullableElementArray = typeFactory.createArrayType(nullableInt, -1);

      // Reader expects non-nullable but writer produces nullable elements
      assertThat(compatibility.isBackwardsCompatible(nonNullableArray, nullableElementArray))
          .isFalse();
    }
  }

  @Nested
  class MapTests {

    @Test
    void givenIdenticalMaps_whenCheckCompatibility_thenReturnsTrue() {
      var map =
          typeFactory.createMapType(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 255),
              typeFactory.createSqlType(SqlTypeName.INTEGER));

      assertThat(compatibility.isBackwardsCompatible(map, map)).isTrue();
    }

    @Test
    void givenMapWithWidenedValueType_whenCheckCompatibility_thenReturnsTrue() {
      var keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
      var writerMap =
          typeFactory.createMapType(keyType, typeFactory.createSqlType(SqlTypeName.INTEGER));
      var readerMap =
          typeFactory.createMapType(keyType, typeFactory.createSqlType(SqlTypeName.BIGINT));

      assertThat(compatibility.isBackwardsCompatible(readerMap, writerMap)).isTrue();
    }

    @Test
    void givenMapWithIncompatibleKeyType_whenCheckCompatibility_thenReturnsFalse() {
      var valueType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var writerMap =
          typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), valueType);
      var readerMap =
          typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.INTEGER), valueType);

      assertThat(compatibility.isBackwardsCompatible(readerMap, writerMap)).isFalse();
    }

    @Test
    void givenMapWithIncompatibleValueType_whenCheckCompatibility_thenReturnsFalse() {
      var keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
      var writerMap =
          typeFactory.createMapType(keyType, typeFactory.createSqlType(SqlTypeName.VARCHAR, 255));
      var readerMap =
          typeFactory.createMapType(keyType, typeFactory.createSqlType(SqlTypeName.BOOLEAN));

      assertThat(compatibility.isBackwardsCompatible(readerMap, writerMap)).isFalse();
    }

    @Test
    void givenMapWithStructValue_whenCheckCompatibility_thenReturnsTrue() {
      var keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
      var struct =
          typeFactory.builder().add("id", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();

      var map = typeFactory.createMapType(keyType, struct);

      assertThat(compatibility.isBackwardsCompatible(map, map)).isTrue();
    }

    @Test
    void givenMapValueNullabilityMismatch_whenCheckCompatibility_thenReturnsFalse() {
      var keyType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
      var nonNullableValue = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var nullableValue = typeFactory.createTypeWithNullability(nonNullableValue, true);

      var writerMap = typeFactory.createMapType(keyType, nullableValue);
      var readerMap = typeFactory.createMapType(keyType, nonNullableValue);

      assertThat(compatibility.isBackwardsCompatible(readerMap, writerMap)).isFalse();
    }
  }

  @Nested
  class EdgeCaseTests {

    @Test
    void givenArrayAndNonArray_whenCheckCompatibility_thenReturnsFalse() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var intArray = typeFactory.createArrayType(intType, -1);

      assertThat(compatibility.isBackwardsCompatible(intArray, intType)).isFalse();
      assertThat(compatibility.isBackwardsCompatible(intType, intArray)).isFalse();
    }

    @Test
    void givenMapAndNonMap_whenCheckCompatibility_thenReturnsFalse() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var map =
          typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), intType);

      assertThat(compatibility.isBackwardsCompatible(map, intType)).isFalse();
      assertThat(compatibility.isBackwardsCompatible(intType, map)).isFalse();
    }

    @Test
    void givenStructAndNonStruct_whenCheckCompatibility_thenReturnsFalse() {
      var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
      var struct = typeFactory.builder().add("id", intType).build();

      assertThat(compatibility.isBackwardsCompatible(struct, intType)).isFalse();
      assertThat(compatibility.isBackwardsCompatible(intType, struct)).isFalse();
    }

    @Test
    void givenTimestampTypes_whenCheckCompatibility_thenReturnsTrue() {
      var timestamp0 = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 0);
      var timestamp3 = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
      var timestamp6 = typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6);

      assertThat(compatibility.isBackwardsCompatible(timestamp3, timestamp0)).isTrue();
      assertThat(compatibility.isBackwardsCompatible(timestamp6, timestamp3)).isTrue();
    }

    @Test
    void givenEmptyStructs_whenCheckCompatibility_thenReturnsTrue() {
      var emptyStruct = typeFactory.builder().build();

      assertThat(compatibility.isBackwardsCompatible(emptyStruct, emptyStruct)).isTrue();
    }

    @Test
    void givenStructWithNullableFieldAndEmptyStruct_whenCheckCompatibility_thenReturnsTrue() {
      var emptyStruct = typeFactory.builder().build();

      var nullableField =
          typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 255), true);
      var structWithNullableField = typeFactory.builder().add("name", nullableField).build();

      // Reader has nullable field, writer has empty struct - compatible
      assertThat(compatibility.isBackwardsCompatible(structWithNullableField, emptyStruct))
          .isTrue();
    }

    @Test
    void givenStructWithNonNullableFieldAndEmptyStruct_whenCheckCompatibility_thenReturnsFalse() {
      var emptyStruct = typeFactory.builder().build();

      var structWithField =
          typeFactory
              .builder()
              .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR, 255))
              .build();

      // Reader has non-nullable field, writer has empty struct - incompatible
      assertThat(compatibility.isBackwardsCompatible(structWithField, emptyStruct)).isFalse();
    }

    @Test
    void givenArrayAndMap_whenCheckCompatibility_thenReturnsFalse() {
      var intArray =
          typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
      var map =
          typeFactory.createMapType(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 255),
              typeFactory.createSqlType(SqlTypeName.INTEGER));

      assertThat(compatibility.isBackwardsCompatible(intArray, map)).isFalse();
      assertThat(compatibility.isBackwardsCompatible(map, intArray)).isFalse();
    }
  }
}
