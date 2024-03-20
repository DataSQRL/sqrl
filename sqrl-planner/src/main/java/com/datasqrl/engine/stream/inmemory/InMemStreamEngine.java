/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.inmemory;

import static com.datasqrl.engine.EngineFeature.STANDARD_STREAM;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.functions.FunctionDefinition;

@Slf4j
public class InMemStreamEngine extends ExecutionEngine.Base implements StreamEngine {

  public InMemStreamEngine() {
    super(InMemoryStreamConfiguration.ENGINE_NAME, ExecutionEngine.Type.STREAM, STANDARD_STREAM);
  }

  @Override
  public boolean supports(FunctionDefinition function) {
    return true;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
                                 ExecutionPipeline pipeline, SqrlFramework relBuilder,
      ErrorCollector errorCollector) {
    throw new UnsupportedOperationException();
  }

}
