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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

@Value
public class FlinkTypeInfoSchemaGenerator implements
    UniversalTable.TypeConverter<TypeInformation>,
    UniversalTable.SchemaConverter<TypeInformation> {

  @Override
  public TypeInformation convertBasic(RelDataType datatype) {
    LogicalType logicalType = FlinkTypeFactory.toLogicalType(datatype);

    DataType dataType = TypeConversions.fromLogicalToDataType(logicalType);
    return TypeConversions.fromDataTypeToLegacyInfo(dataType);
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
