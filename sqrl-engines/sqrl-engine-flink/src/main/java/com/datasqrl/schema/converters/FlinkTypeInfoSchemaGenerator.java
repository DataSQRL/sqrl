/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.schema.UniversalTable;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Value
public class FlinkTypeInfoSchemaGenerator implements
    UniversalTable.TypeConverter<TypeInformation>,
    UniversalTable.SchemaConverter<TypeInformation> {

  @Override
  public TypeInformation convertBasic(RelDataType datatype) {
    if (datatype instanceof RawRelDataType) {
      RawRelDataType rawRelDataType = (RawRelDataType)datatype;
      if (rawRelDataType.getRawType().getOriginatingClass().getName().contains("son")) {
        return BasicTypeInfo.STRING_TYPE_INFO;
      } else if (rawRelDataType.getRawType().getOriginatingClass().getName().contains("ecto")) {
        return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
      }

      throw new RuntimeException();
    }

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
        return BasicTypeInfo.BIG_DEC_TYPE_INFO;
      case REAL:
      case DOUBLE:
        return BasicTypeInfo.DOUBLE_TYPE_INFO;
      case FLOAT:
        return BasicTypeInfo.FLOAT_TYPE_INFO;
      case DATE:
      case TIME:
        return BasicTypeInfo.DATE_TYPE_INFO;
      case ARRAY:
        switch (datatype.getComponentType().getSqlTypeName()) {
          case BOOLEAN:
            return BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO;
          case INTEGER:
            return BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
          case BIGINT:
            return BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
          case FLOAT:
            return BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO;
          case DOUBLE:
            return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
          case CHAR:
            return BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO;
          case VARCHAR:
            return BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
          case BINARY:
            return BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
          default:
            return ObjectArrayTypeInfo.getInfoFor(convertBasic(datatype.getComponentType()));
        }
      case ROW:
        return new RowTypeInfo(datatype.getFieldList().stream()
            .map(f->convertBasic(f.getType()))
            .toArray(TypeInformation[]::new));
      case TIME_WITH_LOCAL_TIME_ZONE:
      case MAP:
        return new MapTypeInfo(convertBasic(datatype.getKeyType()),
            convertBasic(datatype.getValueType()));
      case BINARY:
      case VARBINARY:
      case GEOMETRY:
      case SYMBOL:
      case ANY:
      case NULL:
      default:
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
