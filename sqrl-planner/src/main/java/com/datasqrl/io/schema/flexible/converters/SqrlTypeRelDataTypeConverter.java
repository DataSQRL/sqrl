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
package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.io.schema.SqrlTypeConverter;
import com.datasqrl.io.schema.flexible.type.ArrayType;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.AbstractBasicType;
import com.datasqrl.io.schema.flexible.type.basic.BigIntType;
import com.datasqrl.io.schema.flexible.type.basic.BooleanType;
import com.datasqrl.io.schema.flexible.type.basic.DoubleType;
import com.datasqrl.io.schema.flexible.type.basic.IntervalType;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;
import com.datasqrl.util.CalciteUtil;
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
  public RelDataType visitTimestampType(TimestampType type, Void context) {
    return TypeFactory.makeTimestampType(typeFactory);
  }

  @Override
  public RelDataType visitDoubleType(DoubleType type, Void context) {
    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    //    return typeFactory.createSqlType(SqlTypeName.DECIMAL, 30, 9);
  }

  @Override
  public RelDataType visitBigIntType(BigIntType type, Void context) {
    return typeFactory.createSqlType(SqlTypeName.BIGINT);
  }

  @Override
  public RelDataType visitStringType(StringType type, Void context) {
    return typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
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
        case CHAR:
          return StringType.INSTANCE;
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return TimestampType.INSTANCE;
        case BIGINT:
        case INTEGER:
        case SMALLINT:
        case TINYINT:
          return BigIntType.INSTANCE;
        case BOOLEAN:
          return BooleanType.INSTANCE;
        case DOUBLE:
        case DECIMAL:
        case FLOAT: // sic
        case REAL:
          return DoubleType.INSTANCE;
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
          // TODO: support those 3
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
    var arrayComponent = CalciteUtil.getArrayElementType(datatype);
    if (arrayComponent.isPresent()) {
      return new ArrayType(convert(arrayComponent.get()));
    }
    throw new UnsupportedOperationException(
        "Not a supported data type: " + datatype.getSqlTypeName());
  }
}
