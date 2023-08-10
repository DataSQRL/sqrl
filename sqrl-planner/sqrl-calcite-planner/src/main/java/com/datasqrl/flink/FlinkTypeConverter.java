package com.datasqrl.flink;

import com.datasqrl.calcite.type.BridgingFlinkType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.UnresolvedDataType;

public class FlinkTypeConverter {
  public RelDataType createType(UnresolvedDataType type) {
    //need to resolve it?
//    new FlinkTypeFactory(this.getClass().getClassLoader(), new FlinkT)
//
//        ;
//    type.toDataType();
//
//    BridgingFlinkType bridgingFlinkType = new BridgingFlinkType(
//        type.getConversionClass()
//    );

    return null;

  }
}
