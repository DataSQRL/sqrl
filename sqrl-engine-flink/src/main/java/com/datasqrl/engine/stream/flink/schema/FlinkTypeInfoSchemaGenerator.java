/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.schema;

import com.datasqrl.schema.UniversalTable;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

@Value
public class FlinkTypeInfoSchemaGenerator implements
    UniversalTable.TypeConverter<TypeInformation>,
    UniversalTable.SchemaConverter<TypeInformation> {

  @Override
  public TypeInformation convertBasic(RelDataType datatype) {
    if (datatype instanceof BasicSqlType || datatype instanceof IntervalSqlType) {
      switch (datatype.getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          return BasicTypeInfo.STRING_TYPE_INFO;
        case TIMESTAMP:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return BasicTypeInfo.INSTANT_TYPE_INFO;
        case BIGINT:
          return BasicTypeInfo.LONG_TYPE_INFO;
        case SMALLINT:
        case TINYINT:
        case INTEGER:
          return BasicTypeInfo.INT_TYPE_INFO;
        case BOOLEAN:
          return BasicTypeInfo.BOOLEAN_TYPE_INFO;
        case DECIMAL:
        case REAL:
        case DOUBLE:
          return BasicTypeInfo.DOUBLE_TYPE_INFO;
        case FLOAT:
          return BasicTypeInfo.FLOAT_TYPE_INFO;
        case DATE:
        case TIME:
          return BasicTypeInfo.DATE_TYPE_INFO;

        case TIME_WITH_LOCAL_TIME_ZONE:
        case BINARY:
        case VARBINARY:
        case GEOMETRY:
        case SYMBOL:
        case ANY:
        case NULL:
        default:
      }
    } else if (datatype instanceof IntervalSqlType) {
      IntervalSqlType interval = (IntervalSqlType) datatype;
      switch (interval.getSqlTypeName()) {
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_SECOND:
        case INTERVAL_YEAR:
        case INTERVAL_MINUTE:
        case INTERVAL_HOUR:
        case INTERVAL_DAY:
        case INTERVAL_MONTH:
          throw new UnsupportedOperationException();
      }
    }
    throw new UnsupportedOperationException("Unsupported type: " + datatype);
  }

  @Override
  public TypeInformation nullable(TypeInformation type, boolean nullable) {
    return type; //Does not support nullability
  }

  @Override
  public TypeInformation wrapArray(TypeInformation type) {
    return Types.OBJECT_ARRAY(type);
  }

  @Override
  public TypeInformation nestedTable(List<Pair<String, TypeInformation>> columns) {
    return Types.ROW_NAMED(
        columns.stream().map(Pair::getKey).toArray(i -> new String[i]),
        columns.stream().map(Pair::getValue).toArray(i -> new TypeInformation[i]));
  }

  @Override
  public TypeInformation convertSchema(UniversalTable tblBuilder) {
    return nestedTable(tblBuilder.convert(this));
  }
}
