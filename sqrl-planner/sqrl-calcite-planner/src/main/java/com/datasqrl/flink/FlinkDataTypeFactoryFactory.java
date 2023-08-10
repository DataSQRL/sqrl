package com.datasqrl.flink;

import com.datasqrl.calcite.type.EngineRelDataTypeFactory;
import com.google.auto.service.AutoService;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

@AutoService(EngineRelDataTypeFactory.class)
public class FlinkDataTypeFactoryFactory implements EngineRelDataTypeFactory {

  @Override
  public RelDataTypeFactory create() {
    return new FlinkTypeFactory(this.getClass().getClassLoader(),
        FlinkTypeSystem.INSTANCE);
  }

  @Override
  public boolean matches(Object[] args) {
    return "flink".equals(args[0]);
  }
}
