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

//  private DataType wrapArray(DataType type) {
//    return DataTypes.ARRAY(type);
//  }
//
//  private DataType nestedTable(List<RelDataTypeField> relation) {
//    DataTypes.Field[] fields = new DataTypes.Field[relation.size()];
//    int i = 0;
//    for (RelDataTypeField column : relation) {
//      fields[i++] = DataTypes.FIELD(column.getName(), convertRelDataType(column.getType()));
//    }
//    return DataTypes.ROW(fields);
//  }

  /**
   * We are relying entirely on {@link #convertPrimitive(RelDataType)} for the conversion
   * now but are keeping the old code around in case we encounter issues in the future.
   *
   * TODO: revisit this decision and remove old code
   *
   * @param type
   * @return
   */
  private DataType convertRelDataType(RelDataType type) {
    return convertPrimitive(type);
//    DataType resultType;
//    if (type.isStruct()) {
//      resultType = nestedTable(type.getFieldList());
//    } else if (CalciteUtil.isArray(type)) {
//      resultType = wrapArray(convertRelDataType(CalciteUtil.getArrayElementType(type).get()));
//    } else {
//      resultType = convertPrimitive(type);
//    }
//    return nullable(resultType, type.isNullable());
  }

  @Override
  public Schema convertSchema(UniversalTable table) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    convert(table, schemaBuilder::column);
    return schemaBuilder.build();
  }

  public void convert(UniversalTable table, BiConsumer<String, DataType> consumer) {
    for (RelDataTypeField column : table.getType().getFieldList()) {
      consumer.accept(column.getName(), convertRelDataType(column.getType()));
    }
  }

  public List<Pair<String, DataType>> convertToList(UniversalTable table) {
    List<Pair<String, DataType>> columns = new ArrayList<>();
    convert(table, (name, type) -> columns.add(Pair.of(name, type)));
    return columns;
  }

}
