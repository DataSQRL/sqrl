package com.datasqrl.calcite;

import static org.junit.jupiter.api.Assertions.*;

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

    SqrlFramework framework = new SqrlFramework();

    // Initialize QueryPlanner with mocked framework
    queryPlanner = new QueryPlanner(framework);
  }

  @Nested
  @DisplayName("Test Common Apache Calcite Data Types")
  class CommonCalciteDataTypesTest {

    @Test
    @DisplayName("Parse VARCHAR Type")
    void testParseVarchar() {
      String datatype = "VARCHAR(100)";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
      assertEquals(100, type.getPrecision());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse INTEGER Type")
    void testParseInteger() {
      String datatype = "INTEGER";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse TIMESTAMP Type")
    void testParseTimestamp() {
      String datatype = "TIMESTAMP(3)";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.TIMESTAMP, type.getSqlTypeName());
      assertEquals(3, type.getPrecision());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse TIMESTAMP Type")
    void testParseTimestampLtz() {
      String datatype = "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)";
      RelDataType type = queryPlanner.parseDatatype(datatype);
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
      String datatype = "BOOLEAN";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.BOOLEAN, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse DOUBLE Type")
    void testParseDouble() {
      String datatype = "DOUBLE";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.DOUBLE, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse BIGINT Type")
    void testParseBigInt() {
      String datatype = "BIGINT";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.BIGINT, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }
  }

  @Test
  @DisplayName("Parse 'GraphQLBigInteger' to BIGINT")
  void testParseBigInteger() {
    String datatype = "GraphQLBigInteger";
    RelDataType type = queryPlanner.parseDatatype(datatype);
    assertEquals(SqlTypeName.BIGINT, type.getSqlTypeName());
    assertFalse(type.isNullable());
  }

  @Nested
  @DisplayName("Test Type Aliases")
  class TypeAliasesTest {

    @Test
    @DisplayName("Parse 'string' Alias to VARCHAR")
    void testParseStringAlias() {
      String datatype = "string";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse 'int' Alias to INTEGER")
    void testParseIntAlias() {
      String datatype = "int";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse 'datetime' Alias to TIMESTAMP")
    void testParseDatetimeAlias() {
      String datatype = "datetime";
      RelDataType type = queryPlanner.parseDatatype(datatype);
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
      String datatype = "MAP<STRING, TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)>";
      RelDataType type = queryPlanner.parseDatatype(datatype);
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
      String datatype = "ROW<name STRING, age INT>";
      RelDataType type = queryPlanner.parseDatatype(datatype);
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
      String datatype = "ARRAY<DOUBLE>";
      RelDataType type = queryPlanner.parseDatatype(datatype);
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
      String datatype = "INTEGER NULL";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.INTEGER, type.getSqlTypeName());
      assertTrue(type.isNullable());
    }

    @Test
    @DisplayName("Parse NOT NULL VARCHAR Type")
    void testParseNotNullVarchar() {
      String datatype = "VARCHAR(50) NOT NULL";
      RelDataType type = queryPlanner.parseDatatype(datatype);
      assertEquals(SqlTypeName.VARCHAR, type.getSqlTypeName());
      assertEquals(50, type.getPrecision());
      assertFalse(type.isNullable());
    }

    @Test
    @DisplayName("Parse Not Null MAP Type")
    void testParseNullableMapType() {
      String datatype = "MAP<STRING, INTEGER> NOT NULL";
      RelDataType type = queryPlanner.parseDatatype(datatype);
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
      String datatype = "ROW<name STRING, age INT> NOT NULL";
      RelDataType type = queryPlanner.parseDatatype(datatype);
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
      String datatype = "UNSUPPORTED_TYPE";
      Exception exception = assertThrows(RuntimeException.class, () -> {
        queryPlanner.parseDatatype(datatype);
      });

      String expectedMessage = "Unknown identifier";
      String actualMessage = exception.getMessage();

      assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @DisplayName("Parse Malformed MAP Type")
    void testParseMalformedMapType() {
      String datatype = "MAP<STRING INTEGER>"; // Missing comma
      Exception exception = assertThrows(RuntimeException.class, () -> {
        queryPlanner.parseDatatype(datatype);
      });

      // The exact error message may vary based on parser implementation
      assertTrue(exception.getMessage().contains("SQL parse failed"));
    }
  }
}
