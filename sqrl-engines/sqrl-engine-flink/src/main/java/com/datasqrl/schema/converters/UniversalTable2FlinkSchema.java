/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.calcite.type.VectorType;
import com.datasqrl.schema.UniversalTable;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;


@Value
public class UniversalTable2FlinkSchema implements UniversalTable.TypeConverter<DataType>,
    UniversalTable.SchemaConverter<Schema> {

  //NOTE: Does not include nullable in this call, need to call nullable function
  @Override
  public DataType convertBasic(RelDataType datatype) {
    if (datatype instanceof VectorType) {
      return DataTypes.ARRAY(DataTypes.DOUBLE());
//      FlinkTypeFactory flinkTypeFactory = new FlinkTypeFactory(getClass().getClassLoader(),
//          FlinkTypeSystem.INSTANCE);
//      DataType dataType = DataTypes.of(FlinkVectorType.class).toDataType(
//          FlinkConverter.catalogManager.getDataTypeFactory());
//
//      RelDataType flinkType = flinkTypeFactory
//          .createFieldTypeFromLogicalType(dataType.getLogicalType());
//
//      return dataType;
    }

    switch (datatype.getSqlTypeName()) {
      case VARCHAR:
        return DataTypes.VARCHAR(Integer.MAX_VALUE);
      case CHAR:
        return DataTypes.CHAR(datatype.getPrecision());
      case TIMESTAMP:
        return DataTypes.TIMESTAMP(datatype.getPrecision());
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(datatype.getPrecision());
      case BIGINT:
        return DataTypes.BIGINT();
      case INTEGER:
        return DataTypes.INT();
      case SMALLINT:
        return DataTypes.SMALLINT();
      case TINYINT:
        return DataTypes.TINYINT();
      case BOOLEAN:
        return DataTypes.BOOLEAN();
      case DOUBLE:
        return DataTypes.DOUBLE();
      case DECIMAL:
        return DataTypes.DECIMAL(datatype.getPrecision(), datatype.getScale());
      case FLOAT:
        return DataTypes.FLOAT();
      case REAL:
        return DataTypes.DOUBLE();
      case DATE:
        return DataTypes.DATE();
      case TIME:
        return DataTypes.TIME(datatype.getPrecision());
      case ARRAY:

        return DataTypes.ARRAY(nullable(convertBasic(datatype.getComponentType()), datatype.isNullable()));
      case ROW:
        return DataTypes.ROW(datatype.getFieldList().stream()
            .map(f -> nullable(convertBasic(f.getType()), datatype.isNullable()))
            .toArray(DataType[]::new));
      case TIME_WITH_LOCAL_TIME_ZONE:
      case BINARY:
      case VARBINARY:
      case GEOMETRY:
      case SYMBOL:
      case ANY:
      case NULL:
      default:
        break;
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_YEAR_MONTH:
        throw new UnsupportedOperationException();
      case INTERVAL_SECOND:
        return DataTypes.INTERVAL(DataTypes.SECOND(datatype.getPrecision()));
      case INTERVAL_YEAR:
        return DataTypes.INTERVAL(DataTypes.YEAR(datatype.getPrecision()));
      case INTERVAL_MINUTE:
        return DataTypes.INTERVAL(DataTypes.MINUTE());
      case INTERVAL_HOUR:
        return DataTypes.INTERVAL(DataTypes.HOUR());
      case INTERVAL_DAY:
        return DataTypes.INTERVAL(DataTypes.SECOND(datatype.getPrecision()));
      case INTERVAL_MONTH:
        return DataTypes.INTERVAL(DataTypes.MONTH());
    }
    throw new UnsupportedOperationException("Unsupported type: " + datatype);
  }

  @Override
  public DataType nullable(DataType type, boolean nullable) {
    if (nullable) {
      return type.nullable();
    } else {
      return type.notNull();
    }
  }

  @Override
  public DataType wrapArray(DataType type) {
    return DataTypes.ARRAY(type);
  }

  @Override
  public DataType nestedTable(List<Pair<String, DataType>> columns) {
    DataTypes.Field[] fields = new DataTypes.Field[columns.size()];
    int i = 0;
    for (Pair<String, DataType> column : columns) {
      fields[i++] = DataTypes.FIELD(column.getKey(), column.getValue());
    }
    return DataTypes.ROW(fields);
  }

  @Override
  public Schema convertSchema(UniversalTable tblBuilder) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (Pair<String, DataType> column : tblBuilder.convert(this)) {
      schemaBuilder.column(column.getKey(), column.getValue());
    }
    return schemaBuilder.build();
  }
}
