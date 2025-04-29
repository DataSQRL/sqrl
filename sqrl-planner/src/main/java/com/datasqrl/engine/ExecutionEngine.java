/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import java.util.EnumSet;
import java.util.List;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

/**
 * Describes a physical execution engine and it's capabilities.
 */
public interface ExecutionEngine extends IExecutionEngine {

  /**
   *
   * @param capability
   * @return whether the engine supports the given {@link EngineFeature}
   */
  boolean supports(EngineFeature capability);

  @AllArgsConstructor
  @Getter
  abstract class Base implements ExecutionEngine {

    protected final @NonNull String name;
    protected final @NonNull EngineType type;
    protected final @NonNull EnumSet<EngineFeature> capabilities;

    @Override
    public boolean supports(EngineFeature capability) {
      return capabilities.contains(capability);
    }

  }


}
