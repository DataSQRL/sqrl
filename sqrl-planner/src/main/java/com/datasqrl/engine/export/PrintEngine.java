package com.datasqrl.engine.export;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import java.util.List;

public class PrintEngine implements ExportEngine {

  @Override
  public boolean supports(EngineFeature capability) {
    return false;
  }

  @Override
  public TableConfig getSinkConfig(String sinkName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return PrintEngineFactory.NAME;
  }

  @Override
  public EngineType getType() {
    return EngineType.EXPORT;
  }
}
