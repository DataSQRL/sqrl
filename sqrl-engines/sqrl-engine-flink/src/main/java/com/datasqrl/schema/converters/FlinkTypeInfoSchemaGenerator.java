/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.schema.SchemaConverter;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.SchemaConverterUTB;
import com.datasqrl.util.CalciteUtil;
import java.util.List;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

@Value
public class FlinkTypeInfoSchemaGenerator implements SchemaConverterUTB<TypeInformation>,
    SchemaConverter<TypeInformation> {

  private TypeInformation convertPrimitive(RelDataType datatype) {
    LogicalType logicalType = FlinkTypeFactory.toLogicalType(datatype);

    DataType dataType = TypeConversions.fromLogicalToDataType(logicalType);
    return TypeConversions.fromDataTypeToLegacyInfo(dataType);
  }

  private TypeInformation wrapArray(TypeInformation type) {
    return Types.OBJECT_ARRAY(type);
  }

  private TypeInformation nestedTable(List<RelDataTypeField> relation) {
    return Types.ROW_NAMED(
        relation.stream().map(RelDataTypeField::getName).toArray(i -> new String[i]),
        relation.stream().map(RelDataTypeField::getType).map(this::convertRelDataType)
            .toArray(i -> new TypeInformation[i]));
  }

  private TypeInformation convertRelDataType(RelDataType type) {
    TypeInformation resultType;
    if (type.isStruct()) {
      resultType = nestedTable(type.getFieldList());
    } else if (CalciteUtil.isArray(type)) {
      resultType = wrapArray(convertRelDataType(CalciteUtil.getArrayElementType(type).get()));
    } else {
      resultType = convertPrimitive(type);
    }
    //TypeInformation does not support nullability
    return resultType;
  }

  @Override
  public TypeInformation convertSchema(UniversalTable table) {
    return convertRelDataType(table.getType());
  }

  @Override
  public TypeInformation convertSchema(RelDataType dataType) {
    return convertRelDataType(dataType);
  }
}
