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

//  /**
//   * We currently make the simplifying assumption that an {@link ExecutionPipeline} has a tree
//   * structure. To generalize this to a DAG structure (e.g. to support multiple database engines) we
//   * need to make significant changes to the LPConverter and DAGPlanner. See also
//   * {@link ExecutionPipeline#getStage(ExecutionEngine.Type)}.
//   *
//   * @return Next execution stage in this pipeline
//   */
//  Optional<ExecutionStage> nextStage();

  ExecutionResult execute(EnginePhysicalPlan plan, ErrorCollector errors);

  EnginePhysicalPlan plan(PhysicalDAGPlan.StagePlan plan, List<PhysicalDAGPlan.StageSink> inputs,
      RelBuilder relBuilder, TableSink errorSink);
}
