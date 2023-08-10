
package com.datasqrl.calcite;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.flink.table.planner.plan.schema.StructuredRelDataType;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeFactory extends JavaTypeFactoryImpl {
  private List<RelDataType> types = new ArrayList<>();

  public TypeFactory() {
    super(SqrlTypeSystem.INSTANCE);
  }

  public RelDataType translateToSqrlType(Dialect dialect, RelDataType engineType) {
    RelDataType sqrlType = engineType;
    return sqrlType;
  }

  /**
   * Provides mapping to java types for execution
   */
  @Override
  public Type getJavaClass(RelDataType type) {
    //Flink wrapped types
    if (type instanceof StructuredRelDataType) {
      return ((StructuredRelDataType)type).getStructuredType().getImplementationClass()
          .get();
    }

    //Need to get raw data type mapping for the particular engine

    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      RelDataTypeFactoryImpl.JavaType javaType = (RelDataTypeFactoryImpl.JavaType)type;
      return javaType.getJavaClass();
    } else {
      if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
        switch (type.getSqlTypeName()) {
          case VARCHAR:
          case CHAR:
            return String.class;
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            return Instant.class;
          case DATE:
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE:
          case INTEGER:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
            return type.isNullable() ? Integer.class : Integer.TYPE;
          case TIMESTAMP:
          case BIGINT:
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
            return type.isNullable() ? Long.class : Long.TYPE;
          case SMALLINT:
            return type.isNullable() ? Short.class : Short.TYPE;
          case TINYINT:
            return type.isNullable() ? Byte.class : Byte.TYPE;
          case DECIMAL:
            return BigDecimal.class;
          case BOOLEAN:
            return type.isNullable() ? Boolean.class : Boolean.TYPE;
          case DOUBLE:
          case FLOAT:
            return type.isNullable() ? Double.class : Double.TYPE;
          case REAL:
            return type.isNullable() ? Float.class : Float.TYPE;
          case BINARY:
          case VARBINARY:
            return ByteString.class;
          case GEOMETRY:
            return Geometries.Geom.class;
          case SYMBOL:
            return Enum.class;
          case ANY:
            return Object.class;
          case NULL:
            return Void.class;
        }
      }

      switch (type.getSqlTypeName()) {
        case ROW:
//          assert type instanceof RelRecordType;
//
//          if (type instanceof JavaRecordType) {
//            return ((JavaRecordType)type).clazz;
//          }
//
//          return this.createSyntheticType((RelRecordType)type);
          throw new RuntimeException();
        case MAP:
          return Map.class;
        case ARRAY:
        case MULTISET:
          return List.class;
        default:
          return null;
      }
    }
  }

  /**
   * Registration for flink
   */
  public void registerType(RelDataType type) {
    this.types.add(type);
  }
}
