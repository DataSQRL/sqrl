package com.datasqrl.plan.rules;

import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import lombok.Value;
import org.apache.flink.table.functions.FunctionDefinition;

@Value
public final class IdealExecutionStage implements ExecutionStage {

  public static final ExecutionStage INSTANCE = new IdealExecutionStage();
  public static final String NAME = "IDEAL";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean supportsFeature(EngineFeature capability) {
    return true;
  }

  @Override
  public boolean supportsFunction(FunctionDefinition function) {
    return true;
  }

  @Override
  public ExecutionEngine getEngine() {
    throw new UnsupportedOperationException();
  }

}
