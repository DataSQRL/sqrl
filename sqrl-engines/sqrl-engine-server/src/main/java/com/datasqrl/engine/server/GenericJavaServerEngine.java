package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineCapability.NO_CAPABILITIES;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import java.util.List;
import org.apache.calcite.tools.RelBuilder;

/**
 * A no-feature java server engine.
 */
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  public GenericJavaServerEngine(String engineName) {
    super(engineName, Type.SERVER, NO_CAPABILITIES);
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    //Executed at runtime
    return new ExecutionResult.Message("SUCCESS");
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, RelBuilder relBuilder,
      TableSink errorSink) {

    return new ServerPhysicalPlan(plan.getModel());
  }
}
