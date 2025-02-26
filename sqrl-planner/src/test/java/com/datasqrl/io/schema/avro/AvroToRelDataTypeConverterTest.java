package com.datasqrl.io.schema.avro;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.error.ErrorCollector;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
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
    Schema intSchema = Schema.create(Type.INT);
    RelDataType intType = converter.convert(intSchema);
    assertEquals(SqlTypeName.INTEGER, intType.getSqlTypeName());

    // LONG
    Schema longSchema = Schema.create(Type.LONG);
    RelDataType longType = converter.convert(longSchema);
    assertEquals(SqlTypeName.BIGINT, longType.getSqlTypeName());

    // STRING
    Schema stringSchema = Schema.create(Type.STRING);
    RelDataType stringType = converter.convert(stringSchema);
    assertEquals(SqlTypeName.VARCHAR, stringType.getSqlTypeName());

    // BOOLEAN
    Schema booleanSchema = Schema.create(Type.BOOLEAN);
    RelDataType booleanType = converter.convert(booleanSchema);
    assertEquals(SqlTypeName.BOOLEAN, booleanType.getSqlTypeName());

    // FLOAT
    Schema floatSchema = Schema.create(Type.FLOAT);
    RelDataType floatType = converter.convert(floatSchema);
    assertEquals(SqlTypeName.FLOAT, floatType.getSqlTypeName());

    // DOUBLE
    Schema doubleSchema = Schema.create(Type.DOUBLE);
    RelDataType doubleType = converter.convert(doubleSchema);
    assertEquals(SqlTypeName.DOUBLE, doubleType.getSqlTypeName());

    // BYTES
    Schema bytesSchema = Schema.create(Type.BYTES);
    RelDataType bytesType = converter.convert(bytesSchema);
    assertEquals(SqlTypeName.VARBINARY, bytesType.getSqlTypeName());

    // NULL
    Schema nullSchema = Schema.create(Type.NULL);
    RelDataType nullType = converter.convert(nullSchema);
    assertEquals(SqlTypeName.NULL, nullType.getSqlTypeName());
  }

  @Test
  public void testLogicalTypes() {
    // Decimal (Bytes)
    LogicalTypes.Decimal decimalLogicalType = LogicalTypes.decimal(10, 2);
    Schema decimalSchema = Schema.create(Type.BYTES);
    decimalLogicalType.addToSchema(decimalSchema);
    RelDataType decimalType = converter.convert(decimalSchema);
    assertEquals(SqlTypeName.DECIMAL, decimalType.getSqlTypeName());
    assertEquals(10, decimalType.getPrecision());
    assertEquals(2, decimalType.getScale());

    // Decimal (Fixed)
    Schema fixedDecimalSchema = Schema.createFixed("DecimalFixed", null, null, 16);
    decimalLogicalType.addToSchema(fixedDecimalSchema);
    RelDataType fixedDecimalType = converter.convert(fixedDecimalSchema);
    assertEquals(SqlTypeName.DECIMAL, fixedDecimalType.getSqlTypeName());
    assertEquals(10, fixedDecimalType.getPrecision());
    assertEquals(2, fixedDecimalType.getScale());

    // Date
    Schema dateSchema = Schema.create(Type.INT);
    LogicalTypes.date().addToSchema(dateSchema);
    RelDataType dateType = converter.convert(dateSchema);
    assertEquals(SqlTypeName.DATE, dateType.getSqlTypeName());

    // Time (millis)
    Schema timeMillisSchema = Schema.create(Type.INT);
    LogicalTypes.timeMillis().addToSchema(timeMillisSchema);
    RelDataType timeMillisType = converter.convert(timeMillisSchema);
    assertEquals(SqlTypeName.TIME, timeMillisType.getSqlTypeName());
    assertEquals(0, timeMillisType.getPrecision());

    // Time (micros)
    Schema timeMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.timeMicros().addToSchema(timeMicrosSchema);
    RelDataType timeMicrosType = converter.convert(timeMicrosSchema);
    assertEquals(SqlTypeName.TIME, timeMicrosType.getSqlTypeName());
    assertEquals(0, timeMicrosType.getPrecision());

    // Timestamp (millis)
    Schema timestampMillisSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMillis().addToSchema(timestampMillisSchema);
    RelDataType timestampMillisType = converter.convert(timestampMillisSchema);
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, timestampMillisType.getSqlTypeName());
    assertEquals(3, timestampMillisType.getPrecision());

    // Timestamp (micros)
    Schema timestampMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampMicrosSchema);
    RelDataType timestampMicrosType = converter.convert(timestampMicrosSchema);
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, timestampMicrosType.getSqlTypeName());
    assertEquals(6, timestampMicrosType.getPrecision());

    // Local Timestamp (millis)
    Schema timestampLocalMillisSchema = Schema.create(Type.LONG);
    LogicalTypes.localTimestampMillis().addToSchema(timestampLocalMillisSchema);
    RelDataType timestampLocalMillisType = converter.convert(timestampLocalMillisSchema);
    assertEquals(SqlTypeName.TIMESTAMP, timestampLocalMillisType.getSqlTypeName());
    assertEquals(3, timestampLocalMillisType.getPrecision());

    // Local Timestamp (micros)
    Schema timestampLocalMicrosSchema = Schema.create(Type.LONG);
    LogicalTypes.localTimestampMicros().addToSchema(timestampLocalMicrosSchema);
    RelDataType timestampLocalMicrosType = converter.convert(timestampLocalMicrosSchema);
    assertEquals(SqlTypeName.TIMESTAMP, timestampLocalMicrosType.getSqlTypeName());
    assertEquals(6, timestampLocalMicrosType.getPrecision());

    // UUID
    Schema uuidSchema = Schema.create(Type.STRING);
    LogicalTypes.uuid().addToSchema(uuidSchema);
    RelDataType uuidType = converter.convert(uuidSchema);
    assertEquals(SqlTypeName.VARCHAR, uuidType.getSqlTypeName());
    assertEquals(Integer.MAX_VALUE, uuidType.getPrecision());
  }

  @Test
  public void testUnionTypes() {
    // Union of NULL and INT (nullable INT)
    Schema nullableIntSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Type.NULL), Schema.create(Type.INT)));
    RelDataType nullableIntType = converter.convert(nullableIntSchema);
    assertEquals(SqlTypeName.INTEGER, nullableIntType.getSqlTypeName());
    assertTrue(nullableIntType.isNullable());

    // Union with multiple non-null types (should report an error)
    Schema invalidUnionSchema =
        Schema.createUnion(Arrays.asList(Schema.create(Type.INT), Schema.create(Type.STRING)));
    try {
      RelDataType invalidUnionType = converter.convert(invalidUnionSchema);
      System.out.println(invalidUnionType);
      fail("Expected failure");
    } catch (Exception e) {
    }
    assertTrue(errors.hasErrors());
  }

  @Test
  public void testRecordType() {
    // Create a record schema with various fields
    Schema recordSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .requiredString("name")
            .optionalInt("age")
            .name("balance")
            .type()
            .optional()
            .type(LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Type.BYTES)))
            .endRecord();

    RelDataType recordType = converter.convert(recordSchema);
    assertNotNull(recordType);
    assertEquals(3, recordType.getFieldCount());

    RelDataTypeField nameField = recordType.getFieldList().get(0);
    assertEquals("name", nameField.getName());
    assertEquals(SqlTypeName.VARCHAR, nameField.getType().getSqlTypeName());
    assertFalse(nameField.getType().isNullable());

    RelDataTypeField ageField = recordType.getFieldList().get(1);
    assertEquals("age", ageField.getName());
    assertEquals(SqlTypeName.INTEGER, ageField.getType().getSqlTypeName());
    assertTrue(ageField.getType().isNullable());

    RelDataTypeField balanceField = recordType.getFieldList().get(2);
    assertEquals("balance", balanceField.getName());
    assertEquals(SqlTypeName.DECIMAL, balanceField.getType().getSqlTypeName());
    assertEquals(10, balanceField.getType().getPrecision());
    assertEquals(2, balanceField.getType().getScale());
    assertTrue(balanceField.getType().isNullable());
  }

  @Test
  public void testArrayType() {
    // Array of INT
    Schema arraySchema = Schema.createArray(Schema.create(Type.INT));
    RelDataType arrayType = converter.convert(arraySchema);
    assertEquals(SqlTypeName.ARRAY, arrayType.getSqlTypeName());
    RelDataType elementType = arrayType.getComponentType();
    assertEquals(SqlTypeName.INTEGER, elementType.getSqlTypeName());
  }

  @Test
  public void testMapType() {
    // Map of STRING to LONG
    Schema mapSchema = Schema.createMap(Schema.create(Type.LONG));
    RelDataType mapType = converter.convert(mapSchema);
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
    Schema enumSchema = Schema.createEnum("Color", null, null, symbols);
    RelDataType enumType = converter.convert(enumSchema);
    assertEquals(SqlTypeName.VARCHAR, enumType.getSqlTypeName());
    int expectedLength = symbols.stream().mapToInt(String::length).max().orElse(1);
    //    assertEquals(expectedLength, enumType.getPrecision());
  }

  @Test
  public void testFixedType() {
    // Fixed type without logical type (should map to VARBINARY)
    Schema fixedSchema = Schema.createFixed("FixedType", null, null, 16);
    RelDataType fixedType = converter.convert(fixedSchema);
    assertEquals(SqlTypeName.VARBINARY, fixedType.getSqlTypeName());
    assertEquals(16, fixedType.getPrecision());
  }

  @Test
  public void testComplexRecord() {
    // Record with nested record, array, and map
    Schema innerRecordSchema =
        SchemaBuilder.record("InnerRecord").fields().requiredInt("innerField").endRecord();

    Schema complexRecordSchema =
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

    RelDataType complexRecordType = converter.convert(complexRecordSchema);
    assertNotNull(complexRecordType);
    assertEquals(3, complexRecordType.getFieldCount());

    // Validate nested record field
    RelDataTypeField nestedRecordField = complexRecordType.getFieldList().get(0);
    assertEquals("nestedRecord", nestedRecordField.getName());
    RelDataType nestedRecordType = nestedRecordField.getType();
    assertEquals(SqlTypeName.ROW, nestedRecordType.getSqlTypeName());
    assertEquals(1, nestedRecordType.getFieldCount());
    assertEquals(
        SqlTypeName.INTEGER, nestedRecordType.getFieldList().get(0).getType().getSqlTypeName());

    // Validate array field
    RelDataTypeField intArrayField = complexRecordType.getFieldList().get(1);
    assertEquals("intArray", intArrayField.getName());
    RelDataType intArrayType = intArrayField.getType();
    assertEquals(SqlTypeName.ARRAY, intArrayType.getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, intArrayType.getComponentType().getSqlTypeName());

    // Validate map field
    RelDataTypeField stringMapField = complexRecordType.getFieldList().get(2);
    assertEquals("stringMap", stringMapField.getName());
    RelDataType stringMapType = stringMapField.getType();
    assertEquals(SqlTypeName.MAP, stringMapType.getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, stringMapType.getKeyType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, stringMapType.getValueType().getSqlTypeName());
  }

  @Test
  public void testInvalidSchema() {
    // Invalid schema: Union with multiple non-null types
    Schema invalidUnionSchema =
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
    Schema recordSchema =
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

    RelDataType recordType = converter.convert(recordSchema);
    assertNotNull(recordType);
    assertEquals(2, recordType.getFieldCount());

    RelDataTypeField nullableIntField = recordType.getFieldList().get(0);
    assertTrue(nullableIntField.getType().isNullable());

    RelDataTypeField nonNullableStringField = recordType.getFieldList().get(1);
    assertFalse(nonNullableStringField.getType().isNullable());
  }

  @Test
  public void testNestedLogicalTypes() {
    // Array of timestamps
    Schema timestampSchema = Schema.create(Type.LONG);
    LogicalTypes.timestampMicros().addToSchema(timestampSchema);
    Schema arraySchema = Schema.createArray(timestampSchema);

    RelDataType arrayType = converter.convert(arraySchema);
    assertEquals(SqlTypeName.ARRAY, arrayType.getSqlTypeName());
    RelDataType elementType = arrayType.getComponentType();
    assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, elementType.getSqlTypeName());
    assertEquals(6, elementType.getPrecision());
  }

  @Test
  public void testEmptyRecord() {
    // Empty record
    Schema emptyRecordSchema = SchemaBuilder.record("EmptyRecord").fields().endRecord();
    RelDataType emptyRecordType = converter.convert(emptyRecordSchema);
    assertEquals(SqlTypeName.ROW, emptyRecordType.getSqlTypeName());
  }

  @Test
  public void testUnsupportedLogicalType() {
    // Add an unsupported logical type to a schema
    Schema unsupportedLogicalSchema = Schema.create(Type.INT);
    LogicalTypes.LogicalTypeFactory customFactory =
        schema -> new LogicalType("custom-logical-type");
    LogicalTypes.register("custom-logical-type", customFactory);
    new LogicalType("custom-logical-type").addToSchema(unsupportedLogicalSchema);

    RelDataType type = converter.convert(unsupportedLogicalSchema);
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
    Schema recursiveSchema =
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
      RelDataType type = converter.convert(recursiveSchema);
      fail("Should fail");
    } catch (Exception e) {
    }
    assertTrue(errors.hasErrors());
  }

  @Test
  public void testSchemaWithAliases() {
    // Schema with aliases (should be ignored in conversion)
    Schema aliasedSchema =
        SchemaBuilder.record("OriginalName")
            .aliases("Alias1", "Alias2")
            .fields()
            .requiredString("field")
            .endRecord();

    RelDataType type = converter.convert(aliasedSchema);
    assertNotNull(type);
    assertEquals("OriginalName", aliasedSchema.getName());
    assertEquals(1, type.getFieldCount());
  }
}
