/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.schema.UniversalTable;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;


@Value
public class UniversalTable2FlinkSchema implements UniversalTable.SchemaConverter<Schema> {

  //NOTE: Does not include nullable in this call, need to call nullable function
  public DataType convertPrimitive(RelDataType datatype) {
    LogicalType logicalType = FlinkTypeFactory.toLogicalType(datatype);
    return TypeConversions.fromLogicalToDataType(logicalType);
  }

  public DataType nullable(DataType type, boolean nullable) {
    if (nullable) {
      return type.nullable();
    } else {
      return type.notNull();
    }
  }

  /**
   * We are relying entirely on {@link #convertPrimitive(RelDataType)} for the conversion
   * to DataType, including for array and nested types.
   *
   * If there are issues with this in the future and we need to have more control over the conversion,
   * check the version history of this file in GIT for the old code that did the conversion explicitly.
   *
   * @param type RelDataType to convert to Flink DataType
   * @return
   */
  private DataType convertRelDataType(RelDataType type) {
    return convertPrimitive(type);
  }

  @Override
  public Schema convertSchema(UniversalTable table) {
    return convertSchema(table.getType());
  }

  private Schema convertSchema(RelDataType tableType) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    convert(tableType, schemaBuilder::column);
    return schemaBuilder.build();
  }

  public void convert(RelDataType tableType, BiConsumer<String, DataType> consumer) {
    for (RelDataTypeField column : tableType.getFieldList()) {
      consumer.accept(column.getName(), convertRelDataType(column.getType()));
    }
  }

  public List<Pair<String, DataType>> convertToList(RelDataType tableType) {
    List<Pair<String, DataType>> columns = new ArrayList<>();
    convert(tableType, (name, type) -> columns.add(Pair.of(name, type)));
    return columns;
  }

}
