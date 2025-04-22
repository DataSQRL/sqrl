package com.datasqrl.calcite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class DataTypeTest {

  private QueryPlanner queryPlanner;

  @BeforeEach
  public void setUp() {

    var framework = new SqrlFramework();

    // Initialize QueryPlanner with mocked framework
    queryPlanner = new QueryPlanner(framework);
  }

  @Nested
  @DisplayName("Test Common Apache Calcite Data Types")
  class CommonCalciteDataTypesTest {

    @Test
    @DisplayName("Parse VARCHAR Type")
    void testParseVarchar() {
      var datatype = "VARCHAR(100)";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
      assertEquals(100, type.getPrecision());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse INTEGER Type")
    void testParseInteger() {
      var datatype = "INTEGER";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse TIMESTAMP Type")
    void testParseTimestamp() {
      var datatype = "TIMESTAMP(3)";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
      assertEquals(3, type.getPrecision());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse TIMESTAMP Type")
    void testParseTimestampLtz() {
      var datatype = "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, type.getSqlTypeName());
      assertEquals(3, type.getPrecision());
      assertTrue(type.isNullable());
    }
  }

  @Nested
  @DisplayName("Test Flink Data Types")
  class FlinkDataTypesTest {

    @Test
    @DisplayName("Parse BOOLEAN Type")
    void testParseBoolean() {
      var datatype = "BOOLEAN";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.BOOLEAN, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse DOUBLE Type")
    void testParseDouble() {
      var datatype = "DOUBLE";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.DOUBLE, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse BIGINT Type")
    void testParseBigInt() {
      var datatype = "BIGINT";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.BIGINT, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }
  }

  @Test
  @DisplayName("Parse 'GraphQLBigInteger' to BIGINT")
  void testParseBigInteger() {
    var datatype = "GraphQLBigInteger";
    var type = queryPlanner.parseDatatype(datatype);
    assertEquals(SqlTypeName.BIGINT, type.getSqlTypeName());
    assertFalse(type.isNullable());
  }

  @Nested
  @DisplayName("Test Type Aliases")
  class TypeAliasesTest {

    @Test
    @DisplayName("Parse 'string' Alias to VARCHAR")
    void testParseStringAlias() {
      var datatype = "string";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse 'int' Alias to INTEGER")
    void testParseIntAlias() {
      var datatype = "int";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse 'datetime' Alias to TIMESTAMP")
    void testParseDatetimeAlias() {
      var datatype = "datetime";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
      assertEquals(3, type.getPrecision());
      assertFalse(type.isNullable());
    }
  }

  @Nested
  @DisplayName("Test Complex Types")
  class ComplexTypesTest {

    @Test
    @DisplayName("Parse MAP Type")
    void testParseMapType() {
      var datatype = "MAP<STRING, TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)>";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.MAP, type.getSqlTypeName());

      RelDataType keyType = type.getKeyType();
      RelDataType valueType = type.getValueType();

      assertEquals(SqlTypeName.VARCHAR, keyType.getSqlTypeName());
      assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, valueType.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse ROW Type")
    void testParseRowType() {
      var datatype = "ROW<name STRING, age INT>";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.ROW, type.getSqlTypeName());

      assertEquals(2, type.getFieldCount());
      assertEquals("name", type.getFieldNames().get(0));
      assertEquals(SqlTypeName.VARCHAR, type.getFieldList().get(0).getType().getSqlTypeName());
      assertEquals("age", type.getFieldNames().get(1));
      assertEquals(SqlTypeName.INTEGER, type.getFieldList().get(1).getType().getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse ARRAY Type")
    void testParseArrayType() {
      var datatype = "ARRAY<DOUBLE>";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.ARRAY, type.getSqlTypeName());

      RelDataType elementType = type.getComponentType();
      assertEquals(SqlTypeName.DOUBLE, elementType.getSqlTypeName());
      assertTrue(type.isNullable());
    }
  }

  @Nested
  @DisplayName("Test Nullability")
  class NullabilityTest {

    @Test
    @DisplayName("Parse Nullable INTEGER Type")
    void testParseNullableInteger() {
      var datatype = "INTEGER NULL";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse NOT NULL VARCHAR Type")
    void testParseNotNullVarchar() {
      var datatype = "VARCHAR(50) NOT NULL";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
      assertEquals(50, type.getPrecision());
      assertFalse(type.isNullable());
    }

    @Test
    @DisplayName("Parse Not Null MAP Type")
    void testParseNullableMapType() {
      var datatype = "MAP<STRING, INTEGER> NOT NULL";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.MAP, type.getSqlTypeName());
      assertFalse(type.isNullable());

      RelDataType keyType = type.getKeyType();
      RelDataType valueType = type.getValueType();

      assertEquals(SqlTypeName.VARCHAR, keyType.getSqlTypeName());
      assertEquals(SqlTypeName.INTEGER, valueType.getSqlTypeName());
    }

    @Test
    @DisplayName("Parse NOT NULL ROW Type")
    void testParseNotNullRowType() {
      var datatype = "ROW<name STRING, age INT> NOT NULL";
      var type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.ROW, type.getSqlTypeName());
      assertFalse(type.isNullable());

      assertEquals(2, type.getFieldCount());
      assertEquals("name", type.getFieldNames().get(0));
      assertEquals(SqlTypeName.VARCHAR, type.getFieldList().get(0).getType().getSqlTypeName());
      assertEquals("age", type.getFieldNames().get(1));
      assertEquals(SqlTypeName.INTEGER, type.getFieldList().get(1).getType().getSqlTypeName());
    }
  }

  @Nested
  @DisplayName("Test Invalid Data Types")
  class InvalidDataTypesTest {

    @Test
    @DisplayName("Parse Unsupported Data Type")
    void testParseUnsupportedType() {
      var datatype = "UNSUPPORTED_TYPE";
      Exception exception = assertThrows(RuntimeException.class, () -> {
        queryPlanner.parseDatatype(datatype);
      });

      var expectedMessage = "Unknown identifier";
      var actualMessage = exception.getMessage();

      assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @DisplayName("Parse Malformed MAP Type")
    void testParseMalformedMapType() {
      var datatype = "MAP<STRING INTEGER>"; // Missing comma
      Exception exception = assertThrows(RuntimeException.class, () -> {
        queryPlanner.parseDatatype(datatype);
      });

      // The exact error message may vary based on parser implementation
      assertTrue(exception.getMessage().contains("SQL parse failed"));
    }
  }
}
