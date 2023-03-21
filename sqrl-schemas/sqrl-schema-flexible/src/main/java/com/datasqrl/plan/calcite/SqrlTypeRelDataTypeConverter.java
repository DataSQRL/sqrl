/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite;

import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.schema.TypeUtil;
import com.datasqrl.schema.input.SqrlTypeConverter;
import com.datasqrl.schema.type.ArrayType;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.AbstractBasicType;
import com.datasqrl.schema.type.basic.BooleanType;
import com.datasqrl.schema.type.basic.DateTimeType;
import com.datasqrl.schema.type.basic.FloatType;
import com.datasqrl.schema.type.basic.IntegerType;
import com.datasqrl.schema.type.basic.IntervalType;
import com.datasqrl.schema.type.basic.StringType;
import com.datasqrl.schema.type.basic.UuidType;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

@Value
public class SqrlTypeRelDataTypeConverter implements SqrlTypeConverter<RelDataType> {

  RelDataTypeFactory typeFactory;

  @Override
  public RelDataType visitType(Type type, Void context) {
    throw new UnsupportedOperationException("Should not be called");
  }

  @Override
  public <J> RelDataType visitBasicType(AbstractBasicType<J> type, Void context) {
    throw new IllegalArgumentException("Basic type is not supported: " + type.getName());
  }

  @Override
  public RelDataType visitBooleanType(BooleanType type, Void context) {
    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
  }

  @Override
  public RelDataType visitDateTimeType(DateTimeType type, Void context) {
    return TypeUtil.makeTimestampType(typeFactory);
  }

  @Override
  public RelDataType visitFloatType(FloatType type, Void context) {
    return typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 5);
  }

  @Override
  public RelDataType visitIntegerType(IntegerType type, Void context) {
    return typeFactory.createSqlType(SqlTypeName.BIGINT);
  }

  @Override
  public RelDataType visitStringType(StringType type, Void context) {
    return typeFactory.createSqlType(SqlTypeName.VARCHAR, Short.MAX_VALUE);
  }

  @Override
  public RelDataType visitUuidType(UuidType type, Void context) {
    return TypeUtil.makeUuidType(typeFactory);
  }


  @Override
  public RelDataType visitIntervalType(IntervalType type, Void context) {
    return typeFactory.createSqlIntervalType(
        new SqlIntervalQualifier(TimeUnit.SECOND, 9, null, 3, SqlParserPos.ZERO));
  }

  @Override
  public RelDataType visitArrayType(ArrayType type, Void context) {
    return typeFactory.createArrayType(type.getSubType().accept(this, null), -1L);
  }

  public static Type convert(RelDataType datatype) {
    if (datatype instanceof BasicSqlType || datatype instanceof IntervalSqlType) {
      switch (datatype.getSqlTypeName()) {
        case VARCHAR:
          return StringType.INSTANCE;
        case CHAR:
          if (datatype.getPrecision() == 36) {
            return UuidType.INSTANCE;
          }
          return StringType.INSTANCE;
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return DateTimeType.INSTANCE;
        case BIGINT:
        case INTEGER:
        case SMALLINT:
        case TINYINT:
          return IntegerType.INSTANCE;
        case BOOLEAN:
          return BooleanType.INSTANCE;
        case DOUBLE:
        case DECIMAL:
        case FLOAT: // sic
        case REAL:
          return FloatType.INSTANCE;
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
        case INTERVAL_YEAR:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_MONTH:
          return IntervalType.INSTANCE;
        //TODO: support those 3
        case DATE:
        case TIME:
        case TIME_WITH_LOCAL_TIME_ZONE:

        case BINARY:
        case VARBINARY:
        case GEOMETRY:
        case SYMBOL:
        case ANY:
        case NULL:
        default:
      }
    }
    Optional<RelDataType> arrayComponent = CalciteUtil.getArrayElementType(datatype);
    if (arrayComponent.isPresent()) {
      return new ArrayType(convert(arrayComponent.get()));
    }
    throw new UnsupportedOperationException(
        "Not a supported data type: " + datatype.getSqlTypeName());
  }


}
