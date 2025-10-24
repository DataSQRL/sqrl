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
package com.datasqrl.io.schema.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.error.ErrorCollector;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AvroToRelDataTypeConverterTest {

  private AvroToRelDataTypeConverter converter;
  private ErrorCollector errors;

  @BeforeEach
  void setUp() {
    errors = ErrorCollector.root();
    converter = new AvroToRelDataTypeConverter(errors, false);
  }

  @Test
  void primitiveTypes() {
    // INT
    var intSchema = Schema.create(Type.INT);
    var intType = converter.convert(intSchema);
    assertThat(intType.getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);

    // LONG
    var longSchema = Schema.create(Type.LONG);
    var longType = converter.convert(longSchema);
    assertThat(longType.getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);

    // STRING
    var stringSchema = Schema.create(Type.STRING);
    var stringType = converter.convert(stringSchema);
    assertThat(stringType.getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);

    // BOOLEAN
    var booleanSchema = Schema.create(Type.BOOLEAN);
    var booleanType = converter.convert(booleanSchema);
    assertThat(booleanType.getSqlTypeName()).isEqualTo(SqlTypeName.BOOLEAN);

    // FLOAT
    var floatSchema = Schema.create(Type.FLOAT);
    var floatType = converter.convert(floatSchema);
    assertThat(floatType.getSqlTypeName()).isEqualTo(SqlTypeName.FLOAT);

    // DOUBLE
    var doubleSchema = Schema.create(Type.DOUBLE);
    var doubleType = converter.convert(doubleSchema);
    assertThat(doubleType.getSqlTypeName()).isEqualTo(SqlTypeName.DOUBLE);

    // BYTES
    var bytesSchema = Schema.create(Type.BYTES);
    var bytesType = converter.convert(bytesSchema);
    assertThat(bytesType.getSqlTypeName()).isEqualTo(SqlTypeName.VARBINARY);

    // NULL
    var nullSchema = Schema.create(Type.NULL);
    var nullType = converter.convert(nullSchema);
    assertThat(nullType.getSqlTypeName()).isEqualTo(SqlTypeName.NULL);
  }

  @Test
  void logicalTypes() {
    // Decimal (Bytes)
    var decimalLogicalType = LogicalTypes.decimal(10, 2);
    var decimalSchema = Schema.create(Type.BYTES);
    decimalLogicalType.addToSchema(decimalSchema);
    var decimalType = converter.convert(decimalSchema);
    assertThat(decimalType.getSqlTypeName()).isEqualTo(SqlTypeName.DECIMAL);
    assertThat(decimalType.getPrecision()).isEqualTo(10);
    assertThat(decimalType.getScale()).isEqualTo(2);

    // Decimal (Fixed)
    var fixedDecimalSchema = Schema.createFixed("DecimalFixed", null, null, 16);
    decimalLogicalType.addToSchema(fixedDecimalSchema);
    var fixedDecimalType = converter.convert(fixedDecimalSchema);
    assertThat(fixedDecimalType.getSqlTypeName()).isEqualTo(SqlTypeName.DECIMAL);
    assertThat(fixedDecimalType.getPrecision()).isEqualTo(10);
    assertThat(fixedDecimalType.getScale()).isEqualTo(2);

    // Date
    var dateSchema = Schema.create(Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);
    var dateType = converter.convert(dateSchema);
    assertThat(dateType.getSqlTypeName()).isEqualTo(SqlTypeName.DATE);

    // Time (millis)
    var timeMillisSchema = Schema.create(Type.INT);
    LogicalTypes.timeMillis().addToSchema(timeMillisSchema);
    var timeMillisType = converter.convert(timeMillisSchema);
    assertThat(timeMillisType.getSqlTypeName()).isEqualTo(SqlTypeName.TIME);
    assertThat(timeMillisType.getPrecision()).isEqualTo(0);

    // Time (micros)
    var timeMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.timeMicros().addToSchema(timeMicrosSchema);
    var timeMicrosType = converter.convert(timeMicrosSchema);
    assertThat(timeMicrosType.getSqlTypeName()).isEqualTo(SqlTypeName.TIME);
    assertThat(timeMicrosType.getPrecision()).isEqualTo(0);

    // Timestamp (millis)
    var timestampMillisSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    var timestampMillisType = converter.convert(timestampMillisSchema);
    assertThat(timestampMillisType.getSqlTypeName())
        .isEqualTo(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    assertThat(timestampMillisType.getPrecision()).isEqualTo(3);

    // Timestamp (micros)
    var timestampMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
    var timestampMicrosType = converter.convert(timestampMicrosSchema);
    assertThat(timestampMicrosType.getSqlTypeName())
        .isEqualTo(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    assertThat(timestampMicrosType.getPrecision()).isEqualTo(6);

    // Local Timestamp (millis)
    var timestampLocalMillisSchema = Schema.create(Type.LONG);
    LogicalTypes.localTimestampMillis().addToSchema(timestampLocalMillisSchema);
    var timestampLocalMillisType = converter.convert(timestampLocalMillisSchema);
    assertThat(timestampLocalMillisType.getSqlTypeName()).isEqualTo(SqlTypeName.TIMESTAMP);
    assertThat(timestampLocalMillisType.getPrecision()).isEqualTo(3);

    // Local Timestamp (micros)
    var timestampLocalMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.localTimestampMicros().addToSchema(timestampLocalMicrosSchema);
    var timestampLocalMicrosType = converter.convert(timestampLocalMicrosSchema);
    assertThat(timestampLocalMicrosType.getSqlTypeName()).isEqualTo(SqlTypeName.TIMESTAMP);
    assertThat(timestampLocalMicrosType.getPrecision()).isEqualTo(6);

    // UUID
    var uuidSchema = Schema.create(Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);
    var uuidType = converter.convert(uuidSchema);
    assertThat(uuidType.getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    assertThat(uuidType.getPrecision()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  void testLegacyTimestampMapping() {
    converter = new AvroToRelDataTypeConverter(errors, true);

    // Timestamp (millis)
    var timestampMillisSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    var timestampMillisType = converter.convert(timestampMillisSchema);
    assertThat(timestampMillisType.getSqlTypeName()).isEqualTo(SqlTypeName.TIMESTAMP);
    assertThat(timestampMillisType.getPrecision()).isEqualTo(3);

    // Timestamp (micros)
    var timestampMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
    var timestampMicrosType = converter.convert(timestampMicrosSchema);
    assertThat(timestampMicrosType.getSqlTypeName()).isEqualTo(SqlTypeName.TIMESTAMP);
    assertThat(timestampMicrosType.getPrecision()).isEqualTo(6);
  }

  @Test
  void unionTypes() {
    // Union of NULL and INT (nullable INT)
    var nullableIntSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.INT)));
    var nullableIntType = converter.convert(nullableIntSchema);
    assertThat(nullableIntType.getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);
    assertThat(nullableIntType.isNullable()).isTrue();

    // Union with multiple non-null types (should report an error)
    var invalidUnionSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Type.INT), Schema.create(Type.STRING)));
    try {
      var invalidUnionType = converter.convert(invalidUnionSchema);
      System.out.println(invalidUnionType);
      fail("Expected failure");
    } catch (Exception e) {
    }
    assertThat(errors.hasErrors()).isTrue();
  }

  @Test
  void recordType() {
    // Create a record schema with various fields
    var recordSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .requiredString("name")
            .optionalInt("age")
            .name("balance")
            .type()
            .optional()
            .type(LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Type.BYTES)))
            .endRecord();

    var recordType = converter.convert(recordSchema);
    assertThat(recordType).isNotNull();
    assertThat(recordType.getFieldCount()).isEqualTo(3);

    var nameField = recordType.getFieldList().get(0);
    assertThat(nameField.getName()).isEqualTo("name");
    assertThat(nameField.getType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    assertThat(nameField.getType().isNullable()).isFalse();

    var ageField = recordType.getFieldList().get(1);
    assertThat(ageField.getName()).isEqualTo("age");
    assertThat(ageField.getType().getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);
    assertThat(ageField.getType().isNullable()).isTrue();

    var balanceField = recordType.getFieldList().get(2);
    assertThat(balanceField.getName()).isEqualTo("balance");
    assertThat(balanceField.getType().getSqlTypeName()).isEqualTo(SqlTypeName.DECIMAL);
    assertThat(balanceField.getType().getPrecision()).isEqualTo(10);
    assertThat(balanceField.getType().getScale()).isEqualTo(2);
    assertThat(balanceField.getType().isNullable()).isTrue();
  }

  @Test
  void arrayType() {
    // Array of INT
    var arraySchema = Schema.createArray(Schema.create(Type.INT));
    var arrayType = converter.convert(arraySchema);
    assertThat(arrayType.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    RelDataType elementType = arrayType.getComponentType();
    assertThat(elementType.getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);
  }

  @Test
  void mapType() {
    // Map of STRING to LONG
    var mapSchema = Schema.createMap(Schema.create(Type.LONG));
    var mapType = converter.convert(mapSchema);
    assertThat(mapType.getSqlTypeName()).isEqualTo(SqlTypeName.MAP);
    RelDataType keyType = mapType.getKeyType();
    RelDataType valueType = mapType.getValueType();
    assertThat(keyType.getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    assertThat(valueType.getSqlTypeName()).isEqualTo(SqlTypeName.BIGINT);
  }

  @Test
  void enumType() {
    // Enum with symbols
    List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
    var enumSchema = Schema.createEnum("Color", null, null, symbols);
    var enumType = converter.convert(enumSchema);
    assertThat(enumType.getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    var expectedLength = symbols.stream().mapToInt(String::length).max().orElse(1);
    //    assertEquals(expectedLength, enumType.getPrecision());
  }

  @Test
  void fixedType() {
    // Fixed type without logical type (should map to VARBINARY)
    var fixedSchema = Schema.createFixed("FixedType", null, null, 16);
    var fixedType = converter.convert(fixedSchema);
    assertThat(fixedType.getSqlTypeName()).isEqualTo(SqlTypeName.VARBINARY);
    assertThat(fixedType.getPrecision()).isEqualTo(16);
  }

  @Test
  void complexRecord() {
    // Record with nested record, array, and map
    var innerRecordSchema =
        SchemaBuilder.record("InnerRecord").fields().requiredInt("innerField").endRecord();

    var complexRecordSchema =
        SchemaBuilder.record("ComplexRecord")
            .fields()
            .name("nestedRecord")
            .type(innerRecordSchema)
            .noDefault()
            .name("intArray")
            .type()
            .array()
            .items()
            .intType()
            .noDefault()
            .name("stringMap")
            .type()
            .map()
            .values()
            .stringType()
            .noDefault()
            .endRecord();

    var complexRecordType = converter.convert(complexRecordSchema);
    assertThat(complexRecordType).isNotNull();
    assertThat(complexRecordType.getFieldCount()).isEqualTo(3);

    // Validate nested record field
    var nestedRecordField = complexRecordType.getFieldList().get(0);
    assertThat(nestedRecordField.getName()).isEqualTo("nestedRecord");
    var nestedRecordType = nestedRecordField.getType();
    assertThat(nestedRecordType.getSqlTypeName()).isEqualTo(SqlTypeName.ROW);
    assertThat(nestedRecordType.getFieldCount()).isEqualTo(1);
    assertThat(nestedRecordType.getFieldList().get(0).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.INTEGER);

    // Validate array field
    var intArrayField = complexRecordType.getFieldList().get(1);
    assertThat(intArrayField.getName()).isEqualTo("intArray");
    var intArrayType = intArrayField.getType();
    assertThat(intArrayType.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    assertThat(intArrayType.getComponentType().getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER);

    // Validate map field
    var stringMapField = complexRecordType.getFieldList().get(2);
    assertThat(stringMapField.getName()).isEqualTo("stringMap");
    var stringMapType = stringMapField.getType();
    assertThat(stringMapType.getSqlTypeName()).isEqualTo(SqlTypeName.MAP);
    assertThat(stringMapType.getKeyType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
    assertThat(stringMapType.getValueType().getSqlTypeName()).isEqualTo(SqlTypeName.VARCHAR);
  }

  @Test
  void invalidSchema() {
    // Invalid schema: Union with multiple non-null types
    var invalidUnionSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Type.INT), Schema.create(Type.STRING)));
    RelDataType type = null;
    try {
      type = converter.convert(invalidUnionSchema);
      System.out.println(type);
      fail("Expected failure");
    } catch (Exception e) {
    }
    assertThat(type).isNull();
    assertThat(errors.hasErrors()).isTrue();
  }

  @Test
  void nullableFieldsInRecord() {
    // Record with nullable fields
    var recordSchema =
        SchemaBuilder.record("NullableRecord")
            .fields()
            .name("nullableInt")
            .type()
            .unionOf()
            .nullType()
            .and()
            .intType()
            .endUnion()
            .noDefault()
            .name("nonNullableString")
            .type()
            .stringType()
            .noDefault()
            .endRecord();

    var recordType = converter.convert(recordSchema);
    assertThat(recordType).isNotNull();
    assertThat(recordType.getFieldCount()).isEqualTo(2);

    var nullableIntField = recordType.getFieldList().get(0);
    assertThat(nullableIntField.getType().isNullable()).isTrue();

    var nonNullableStringField = recordType.getFieldList().get(1);
    assertThat(nonNullableStringField.getType().isNullable()).isFalse();
  }

  @Test
  void nestedLogicalTypes() {
    // Array of timestamps
    var timestampSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampSchema);
    var arraySchema = Schema.createArray(timestampSchema);

    var arrayType = converter.convert(arraySchema);
    assertThat(arrayType.getSqlTypeName()).isEqualTo(SqlTypeName.ARRAY);
    RelDataType elementType = arrayType.getComponentType();
    assertThat(elementType.getSqlTypeName()).isEqualTo(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    assertThat(elementType.getPrecision()).isEqualTo(6);
  }

  @Test
  void emptyRecord() {
    // Empty record
    var emptyRecordSchema = SchemaBuilder.record("EmptyRecord").fields().endRecord();
    var emptyRecordType = converter.convert(emptyRecordSchema);
    assertThat(emptyRecordType.getSqlTypeName()).isEqualTo(SqlTypeName.ROW);
  }

  @Test
  void unsupportedLogicalType() {
    // Add an unsupported logical type to a schema
    var unsupportedLogicalSchema = Schema.create(Type.INT);
    LogicalTypes.LogicalTypeFactory customFactory =
        schema -> new LogicalType("custom-logical-type");
    LogicalTypes.register("custom-logical-type", customFactory);
    new LogicalType("custom-logical-type").addToSchema(unsupportedLogicalSchema);

    var type = converter.convert(unsupportedLogicalSchema);
    assertThat(type.getSqlTypeName()).isEqualTo(SqlTypeName.INTEGER); // Should default to base type
  }

  @Test
  void nullSchema() {
    // Test conversion with null schema (should throw NullPointerException)
    assertThatThrownBy(() -> converter.convert(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void recursiveSchema() {
    // Recursive record schema (simplified for test)
    var recursiveSchema =
        SchemaBuilder.record("Node")
            .fields()
            .name("value")
            .type()
            .intType()
            .noDefault()
            .name("next")
            .type()
            .optional()
            .type("Node")
            .endRecord();

    // Since the converter doesn't handle recursive schemas, it should report an error
    try {
      var type = converter.convert(recursiveSchema);
      fail("Should fail");
    } catch (Exception e) {
    }
    assertThat(errors.hasErrors()).isTrue();
  }

  @Test
  void schemaWithAliases() {
    // Schema with aliases (should be ignored in conversion)
    var aliasedSchema =
        SchemaBuilder.record("OriginalName")
            .aliases("Alias1", "Alias2")
            .fields()
            .requiredString("field")
            .endRecord();

    var type = converter.convert(aliasedSchema);
    assertThat(type).isNotNull();
    assertThat(aliasedSchema.getName()).isEqualTo("OriginalName");
    assertThat(type.getFieldCount()).isEqualTo(1);
  }

  @Test
  void typeReuseVsRecursiveSchema() {
    // Named reusable schema (non-recursive)
    var sharedRecord = SchemaBuilder.record("Shared").fields().requiredString("label").endRecord();

    // Schema that reuses 'Shared' twice (no recursion)
    var reusableSchema =
        SchemaBuilder.record("Container")
            .fields()
            .name("first")
            .type(sharedRecord)
            .noDefault()
            .name("second")
            .type(sharedRecord)
            .noDefault()
            .endRecord();

    var reusableType = converter.convert(reusableSchema);
    assertThat(reusableType).isNotNull();
    assertThat(reusableType.getFieldCount()).isEqualTo(2);
    assertThat(reusableType.getFieldList().get(0).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.ROW);
    assertThat(reusableType.getFieldList().get(1).getType().getSqlTypeName())
        .isEqualTo(SqlTypeName.ROW);

    // Recursive schema: a 'Node' record that refers to itself
    var recursiveSchema =
        SchemaBuilder.record("Node")
            .fields()
            .requiredInt("value")
            .name("next")
            .type()
            .optional()
            .type("Node")
            .endRecord();

    var recursiveErrors = ErrorCollector.root();
    var recursiveConverter = new AvroToRelDataTypeConverter(recursiveErrors, false);

    assertThatThrownBy(() -> recursiveConverter.convert(recursiveSchema))
        .isInstanceOf(RuntimeException.class);
    assertThat(recursiveErrors.hasErrors()).isTrue();
  }
}
