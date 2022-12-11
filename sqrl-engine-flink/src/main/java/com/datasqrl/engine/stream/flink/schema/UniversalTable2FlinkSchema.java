/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.schema;

import com.datasqrl.schema.UniversalTableBuilder;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;


@Value
public class UniversalTable2FlinkSchema implements UniversalTableBuilder.TypeConverter<DataType>,
    UniversalTableBuilder.SchemaConverter<Schema> {

  @Override
  public DataType convertBasic(RelDataType datatype) {
    if (datatype instanceof BasicSqlType || datatype instanceof IntervalSqlType) {
      switch (datatype.getSqlTypeName()) {
        case VARCHAR:
          return DataTypes.STRING();
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
  public Schema convertSchema(UniversalTableBuilder tblBuilder) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (Pair<String, DataType> column : tblBuilder.convert(this)) {
      schemaBuilder.column(column.getKey(), column.getValue());
    }
    return schemaBuilder.build();
  }
}
