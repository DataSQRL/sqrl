package com.datasqrl.calcite;

import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;


public class SqrlTypeSystem extends RelDataTypeSystemImpl {
  public static final RelDataTypeSystem INSTANCE =
      new SqrlTypeSystem();

//  @Delegate
//  FlinkTypeSystem flinkTypeSystem;

  public SqrlTypeSystem() {
//    flinkTypeSystem = FlinkTypeSystem.INSTANCE;
  }
}
