/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.tools.RelBuilder;

public interface ExecutionStage {

  String getName();

  default boolean supportsAll(Collection<EngineCapability> capabilities) {
    return capabilities.stream().allMatch(this::supports);
  }

  boolean supports(EngineCapability capability);

  default boolean isRead() {
    return getEngine().getType().isRead();
  }

  default boolean isWrite() {
    return getEngine().getType().isWrite();
  }

  ExecutionEngine getEngine();

}
