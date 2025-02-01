/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;

import java.util.Collection;

import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;

public interface ExecutionStage {

  String getName();

  default boolean supportsAllFeatures(Collection<EngineFeature> capabilities) {
    return capabilities.stream().allMatch(this::supportsFeature);
  }

  default EngineType getType() {
    return getEngine().getType();
  }

  boolean supportsFeature(EngineFeature capability);

//  boolean supportsFunction(FunctionDefinition function);

  default boolean isRead() {
    return getEngine().getType().isRead();
  }

  default boolean isWrite() {
    return getEngine().getType().isWrite();
  }

  default boolean isCompute() { return getEngine().getType().isCompute(); }

  ExecutionEngine getEngine();

  default boolean supportsFunction(SqlOperator operator) {
    return true;
  }
}
