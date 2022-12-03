package com.datasqrl.plan.calcite;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaRecordType;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

public class SqrlTypeFactory extends JavaTypeFactoryImpl {

  public SqrlTypeFactory(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  public RelDataType createSqlType(SqlTypeName typeName) {
    if (typeName.allowsPrec()) {
      return this.createSqlType(typeName, this.typeSystem.getDefaultPrecision(typeName));
    } else {
      RelDataType newType = new BasicSqlType(this.typeSystem, typeName);

      //TODO: Unknown why flink has issues with nullable functions
      return this.createTypeWithNullability(newType, true);
//      return this.canonize(newType)
//          ;
    }
  }

  @Override
  public Charset getDefaultCharset() {
    return Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
  }

  @Override public Type getJavaClass(RelDataType type) {
    if (type instanceof JavaType) {
      JavaType javaType = (JavaType) type;
      return javaType.getJavaClass();
    }
    if (type instanceof BasicSqlType || type instanceof IntervalSqlType) {
      switch (type.getSqlTypeName()) {
        case VARCHAR:
        case CHAR:
          return String.class;
        case DATE:
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:
        case INTEGER:
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
          return type.isNullable() ? Integer.class : int.class;
        case TIMESTAMP:
          return type.isNullable() ? Long.class : long.class;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return Instant.class;
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
          return type.isNullable() ? Long.class : long.class;
        case SMALLINT:
          return type.isNullable() ? Short.class : short.class;
        case TINYINT:
          return type.isNullable() ? Byte.class : byte.class;
        case DECIMAL:
          return BigDecimal.class;
        case BOOLEAN:
          return type.isNullable() ? Boolean.class : boolean.class;
        case DOUBLE:
        case FLOAT: // sic
          return type.isNullable() ? Double.class : double.class;
        case REAL:
          return type.isNullable() ? Float.class : float.class;
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
        default:
          break;
      }
    }
    switch (type.getSqlTypeName()) {
      case ROW:
        assert type instanceof RelRecordType;
        if (type instanceof JavaRecordType) {
          return ((JavaRecordType) type).getClass();
        } else {
          return createSyntheticType(List.of(((RelRecordType) type).getClass()));
        }
      case MAP:
        return Map.class;
      case ARRAY:
      case MULTISET:
        return List.class;
      default:
        break;
    }
    return Object.class;
  }
}
