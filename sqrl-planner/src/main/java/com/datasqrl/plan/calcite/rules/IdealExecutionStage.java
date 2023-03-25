package com.datasqrl.plan.calcite.rules;

import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import java.util.List;
import lombok.Value;
import org.apache.calcite.tools.RelBuilder;

@Value
public final class IdealExecutionStage implements ExecutionStage {

  public static final ExecutionStage INSTANCE = new IdealExecutionStage();

  @Override
  public boolean supports(EngineCapability capability) {
    return true;
  }

  @Override
  public ExecutionEngine getEngine() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, RelBuilder relBuilder,
      TableSink errorSink) {
    throw new UnsupportedOperationException();
  }
}
