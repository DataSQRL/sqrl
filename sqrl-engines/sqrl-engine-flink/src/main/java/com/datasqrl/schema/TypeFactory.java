/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

public class TypeFactory {

  public static RelDataTypeFactory getTypeFactory() {
    return new FlinkTypeFactory(ClassLoader.getSystemClassLoader(), getTypeSystem());
  }

  public static RelDataTypeSystem getTypeSystem() {
    return FlinkTypeSystem.INSTANCE;
  }
}
