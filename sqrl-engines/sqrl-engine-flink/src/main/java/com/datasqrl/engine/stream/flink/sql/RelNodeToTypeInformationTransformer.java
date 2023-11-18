package com.datasqrl.engine.stream.flink.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

public class RelNodeToTypeInformationTransformer {

  public TypeInformation transform(RelNode relNode) {
    LogicalType logicalType = FlinkTypeFactory.toLogicalType(relNode.getRowType());
    DataType dataType = TypeConversions.fromLogicalToDataType(logicalType);
    return TypeConversions.fromDataTypeToLegacyInfo(dataType);
  }
}
