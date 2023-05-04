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
import java.util.List;
import lombok.Value;
import org.apache.calcite.tools.RelBuilder;

@Value
public class EngineStage implements ExecutionStage {

  ExecutionEngine engine;

  @Override
  public String getName() {
    return engine.getName();
  }

  @Override
  public boolean supports(EngineCapability capability) {
    return engine.supports(capability);
  }
  
}
