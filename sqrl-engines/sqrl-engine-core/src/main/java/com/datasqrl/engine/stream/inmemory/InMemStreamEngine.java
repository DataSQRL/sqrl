/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.inmemory;

import static com.datasqrl.engine.EngineFeature.STANDARD_STREAM;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
//
//  @Override
//  public JobBuilder createDataMonitor() {
//    return new JobBuilder();
//  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
                                 ExecutionPipeline pipeline, SqrlFramework relBuilder, TableSink errorSink,
      ErrorCollector errorCollector) {
    throw new UnsupportedOperationException();
  }

}
