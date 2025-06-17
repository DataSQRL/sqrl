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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

public class AvroToRelDataTypeConverterTest {

  private AvroToRelDataTypeConverter converter;
  private ErrorCollector errors;

  @BeforeEach
  public void setUp() {
    errors = ErrorCollector.root();
    converter = new AvroToRelDataTypeConverter(errors, false);
  }

  @Test
  public void testPrimitiveTypes() {
    // INT
    var intSchema = Schema.create(Type.INT);
    var intType = converter.convert(intSchema);
    assertEquals(SqlTypeName.INTEGER, intType.getSqlTypeName());

    // LONG
    var longSchema = Schema.create(Type.LONG);
    var longType = converter.convert(longSchema);
    assertEquals(SqlTypeName.BIGINT, longType.getSqlTypeName());

    // STRING
    var stringSchema = Schema.create(Type.STRING);
    var stringType = converter.convert(stringSchema);
    assertEquals(SqlTypeName.VARCHAR, stringType.getSqlTypeName());

    // BOOLEAN
    var booleanSchema = Schema.create(Type.BOOLEAN);
    var booleanType = converter.convert(booleanSchema);
    assertEquals(SqlTypeName.BOOLEAN, booleanType.getSqlTypeName());

    // FLOAT
    var floatSchema = Schema.create(Type.FLOAT);
    var floatType = converter.convert(floatSchema);
    assertEquals(SqlTypeName.FLOAT, floatType.getSqlTypeName());

    // DOUBLE
    var doubleSchema = Schema.create(Type.DOUBLE);
    var doubleType = converter.convert(doubleSchema);
    assertEquals(SqlTypeName.DOUBLE, doubleType.getSqlTypeName());

    // BYTES
    var bytesSchema = Schema.create(Type.BYTES);
    var bytesType = converter.convert(bytesSchema);
    assertEquals(SqlTypeName.VARBINARY, bytesType.getSqlTypeName());

    // NULL
    var nullSchema = Schema.create(Type.NULL);
    var nullType = converter.convert(nullSchema);
    assertEquals(SqlTypeName.NULL, nullType.getSqlTypeName());
  }

  @Test
  public void testLogicalTypes() {
    // Decimal (Bytes)
    var decimalLogicalType = LogicalTypes.decimal(10, 2);
    var decimalSchema = Schema.create(Type.BYTES);
    decimalLogicalType.addToSchema(decimalSchema);
    var decimalType = converter.convert(decimalSchema);
    assertEquals(SqlTypeName.DECIMAL, decimalType.getSqlTypeName());
    assertEquals(10, decimalType.getPrecision());
    assertEquals(2, decimalType.getScale());

    // Decimal (Fixed)
    var fixedDecimalSchema = Schema.createFixed("DecimalFixed", null, null, 16);
    decimalLogicalType.addToSchema(fixedDecimalSchema);
    var fixedDecimalType = converter.convert(fixedDecimalSchema);
    assertEquals(SqlTypeName.DECIMAL, fixedDecimalType.getSqlTypeName());
    assertEquals(10, fixedDecimalType.getPrecision());
    assertEquals(2, fixedDecimalType.getScale());

    // Date
    var dateSchema = Schema.create(Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);
    var dateType = converter.convert(dateSchema);
    assertEquals(SqlTypeName.DATE, dateType.getSqlTypeName());

    // Time (millis)
    var timeMillisSchema = Schema.create(Type.INT);
    LogicalTypes.timeMillis().addToSchema(timeMillisSchema);
    var timeMillisType = converter.convert(timeMillisSchema);
    assertEquals(SqlTypeName.TIME, timeMillisType.getSqlTypeName());
    assertEquals(0, timeMillisType.getPrecision());

    // Time (micros)
    var timeMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.timeMicros().addToSchema(timeMicrosSchema);
    var timeMicrosType = converter.convert(timeMicrosSchema);
    assertEquals(SqlTypeName.TIME, timeMicrosType.getSqlTypeName());
    assertEquals(0, timeMicrosType.getPrecision());

    // Timestamp (millis)
    var timestampMillisSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    var timestampMillisType = converter.convert(timestampMillisSchema);
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, timestampMillisType.getSqlTypeName());
    assertEquals(3, timestampMillisType.getPrecision());

    // Timestamp (micros)
    var timestampMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
    var timestampMicrosType = converter.convert(timestampMicrosSchema);
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, timestampMicrosType.getSqlTypeName());
    assertEquals(6, timestampMicrosType.getPrecision());

    // Local Timestamp (millis)
    var timestampLocalMillisSchema = Schema.create(Type.LONG);
    LogicalTypes.localTimestampMillis().addToSchema(timestampLocalMillisSchema);
    var timestampLocalMillisType = converter.convert(timestampLocalMillisSchema);
    assertEquals(SqlTypeName.TIMESTAMP, timestampLocalMillisType.getSqlTypeName());
    assertEquals(3, timestampLocalMillisType.getPrecision());

    // Local Timestamp (micros)
    var timestampLocalMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.localTimestampMicros().addToSchema(timestampLocalMicrosSchema);
    var timestampLocalMicrosType = converter.convert(timestampLocalMicrosSchema);
    assertEquals(SqlTypeName.TIMESTAMP, timestampLocalMicrosType.getSqlTypeName());
    assertEquals(6, timestampLocalMicrosType.getPrecision());

    // UUID
    var uuidSchema = Schema.create(Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);
    var uuidType = converter.convert(uuidSchema);
    assertEquals(SqlTypeName.VARCHAR, uuidType.getSqlTypeName());
    assertEquals(Integer.MAX_VALUE, uuidType.getPrecision());
  }

  @Test
  public void testUnionTypes() {
    // Union of NULL and INT (nullable INT)
    var nullableIntSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.INT)));
    var nullableIntType = converter.convert(nullableIntSchema);
    assertEquals(SqlTypeName.INTEGER, nullableIntType.getSqlTypeName());
    assertTrue(nullableIntType.isNullable());

    // Union with multiple non-null types (should report an error)
    var invalidUnionSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Type.INT), Schema.create(Type.STRING)));
    try {
      var invalidUnionType = converter.convert(invalidUnionSchema);
      System.out.println(invalidUnionType);
      fail("Expected failure");
    } catch (Exception e) {
    }
    assertTrue(errors.hasErrors());
  }

  @Test
  public void testRecordType() {
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
    assertNotNull(recordType);
    assertEquals(3, recordType.getFieldCount());

    var nameField = recordType.getFieldList().get(0);
    assertEquals("name", nameField.getName());
    assertEquals(SqlTypeName.VARCHAR, nameField.getType().getSqlTypeName());
    assertFalse(nameField.getType().isNullable());

    var ageField = recordType.getFieldList().get(1);
    assertEquals("age", ageField.getName());
    assertEquals(SqlTypeName.INTEGER, ageField.getType().getSqlTypeName());
    assertTrue(ageField.getType().isNullable());

    var balanceField = recordType.getFieldList().get(2);
    assertEquals("balance", balanceField.getName());
    assertEquals(SqlTypeName.DECIMAL, balanceField.getType().getSqlTypeName());
    assertEquals(10, balanceField.getType().getPrecision());
    assertEquals(2, balanceField.getType().getScale());
    assertTrue(balanceField.getType().isNullable());
  }

  @Test
  public void testArrayType() {
    // Array of INT
    var arraySchema = Schema.createArray(Schema.create(Type.INT));
    var arrayType = converter.convert(arraySchema);
    assertEquals(SqlTypeName.ARRAY, arrayType.getSqlTypeName());
    RelDataType elementType = arrayType.getComponentType();
    assertEquals(SqlTypeName.INTEGER, elementType.getSqlTypeName());
  }

  @Test
  public void testMapType() {
    // Map of STRING to LONG
    var mapSchema = Schema.createMap(Schema.create(Type.LONG));
    var mapType = converter.convert(mapSchema);
    assertEquals(SqlTypeName.MAP, mapType.getSqlTypeName());
    RelDataType keyType = mapType.getKeyType();
    RelDataType valueType = mapType.getValueType();
    assertEquals(SqlTypeName.VARCHAR, keyType.getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, valueType.getSqlTypeName());
  }

  @Test
  public void testEnumType() {
    // Enum with symbols
    List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
    var enumSchema = Schema.createEnum("Color", null, null, symbols);
    var enumType = converter.convert(enumSchema);
    assertEquals(SqlTypeName.VARCHAR, enumType.getSqlTypeName());
    var expectedLength = symbols.stream().mapToInt(String::length).max().orElse(1);
    //    assertEquals(expectedLength, enumType.getPrecision());
  }

  @Test
  public void testFixedType() {
    // Fixed type without logical type (should map to VARBINARY)
    var fixedSchema = Schema.createFixed("FixedType", null, null, 16);
    var fixedType = converter.convert(fixedSchema);
    assertEquals(SqlTypeName.VARBINARY, fixedType.getSqlTypeName());
    assertEquals(16, fixedType.getPrecision());
  }

  @Test
  public void testComplexRecord() {
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
    assertNotNull(complexRecordType);
    assertEquals(3, complexRecordType.getFieldCount());

    // Validate nested record field
    var nestedRecordField = complexRecordType.getFieldList().get(0);
    assertEquals("nestedRecord", nestedRecordField.getName());
    var nestedRecordType = nestedRecordField.getType();
    assertEquals(SqlTypeName.ROW, nestedRecordType.getSqlTypeName());
    assertEquals(1, nestedRecordType.getFieldCount());
    assertEquals(
        SqlTypeName.INTEGER, nestedRecordType.getFieldList().get(0).getType().getSqlTypeName());

    // Validate array field
    var intArrayField = complexRecordType.getFieldList().get(1);
    assertEquals("intArray", intArrayField.getName());
    var intArrayType = intArrayField.getType();
    assertEquals(SqlTypeName.ARRAY, intArrayType.getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, intArrayType.getComponentType().getSqlTypeName());

    // Validate map field
    var stringMapField = complexRecordType.getFieldList().get(2);
    assertEquals("stringMap", stringMapField.getName());
    var stringMapType = stringMapField.getType();
    assertEquals(SqlTypeName.MAP, stringMapType.getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, stringMapType.getKeyType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, stringMapType.getValueType().getSqlTypeName());
  }

  @Test
  public void testInvalidSchema() {
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
    assertNull(type);
    assertTrue(errors.hasErrors());
  }

  @Test
  public void testNullableFieldsInRecord() {
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
    assertNotNull(recordType);
    assertEquals(2, recordType.getFieldCount());

    var nullableIntField = recordType.getFieldList().get(0);
    assertTrue(nullableIntField.getType().isNullable());

    var nonNullableStringField = recordType.getFieldList().get(1);
    assertFalse(nonNullableStringField.getType().isNullable());
  }

  @Test
  public void testNestedLogicalTypes() {
    // Array of timestamps
    var timestampSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampSchema);
    var arraySchema = Schema.createArray(timestampSchema);

    var arrayType = converter.convert(arraySchema);
    assertEquals(SqlTypeName.ARRAY, arrayType.getSqlTypeName());
    RelDataType elementType = arrayType.getComponentType();
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, elementType.getSqlTypeName());
    assertEquals(6, elementType.getPrecision());
  }

  @Test
  public void testEmptyRecord() {
    // Empty record
    var emptyRecordSchema = SchemaBuilder.record("EmptyRecord").fields().endRecord();
    var emptyRecordType = converter.convert(emptyRecordSchema);
    assertEquals(SqlTypeName.ROW, emptyRecordType.getSqlTypeName());
  }

  @Test
  public void testUnsupportedLogicalType() {
    // Add an unsupported logical type to a schema
    var unsupportedLogicalSchema = Schema.create(Type.INT);
    LogicalTypes.LogicalTypeFactory customFactory =
        schema -> new LogicalType("custom-logical-type");
    LogicalTypes.register("custom-logical-type", customFactory);
    new LogicalType("custom-logical-type").addToSchema(unsupportedLogicalSchema);

    var type = converter.convert(unsupportedLogicalSchema);
    assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName()); // Should default to base type
  }

  @Test
  public void testNullSchema() {
    // Test conversion with null schema (should throw NullPointerException)
    assertThrows(NullPointerException.class, () -> converter.convert(null));
  }

  @Test
  public void testRecursiveSchema() {
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
    assertTrue(errors.hasErrors());
  }

  @Test
  public void testSchemaWithAliases() {
    // Schema with aliases (should be ignored in conversion)
    var aliasedSchema =
        SchemaBuilder.record("OriginalName")
            .aliases("Alias1", "Alias2")
            .fields()
            .requiredString("field")
            .endRecord();

    var type = converter.convert(aliasedSchema);
    assertNotNull(type);
    assertEquals("OriginalName", aliasedSchema.getName());
    assertEquals(1, type.getFieldCount());
  }

  @Test
  public void testTypeReuseVsRecursiveSchema() {
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
    assertNotNull(reusableType);
    assertEquals(2, reusableType.getFieldCount());
    assertEquals(SqlTypeName.ROW, reusableType.getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(SqlTypeName.ROW, reusableType.getFieldList().get(1).getType().getSqlTypeName());

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

    assertThrows(RuntimeException.class, () -> recursiveConverter.convert(recursiveSchema));
    assertTrue(recursiveErrors.hasErrors());
  }
}
