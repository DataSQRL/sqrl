/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.schema.UniversalTable;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;


@Value
public class UniversalTable2FlinkSchema implements UniversalTable.TypeConverter<DataType>,
    UniversalTable.SchemaConverter<Schema> {

  //NOTE: Does not include nullable in this call, need to call nullable function
  @Override
  public DataType convertBasic(RelDataType datatype) {
    LogicalType logicalType = FlinkTypeFactory.toLogicalType(datatype);
    return TypeConversions.fromLogicalToDataType(logicalType);
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
