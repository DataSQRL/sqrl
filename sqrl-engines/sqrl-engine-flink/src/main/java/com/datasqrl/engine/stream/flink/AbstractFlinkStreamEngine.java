/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.EngineCapability.STANDARD_STREAM;

import com.datasqrl.FlinkEnvironmentBuilder;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.*;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkPhysicalPlanner;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkExecutablePlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

@Slf4j
public abstract class AbstractFlinkStreamEngine extends ExecutionEngine.Base implements
    StreamEngine {

  public static final EnumSet<EngineCapability> FLINK_CAPABILITIES = STANDARD_STREAM;
  private final SqrlConfig config;

  public AbstractFlinkStreamEngine(SqrlConfig config) {
    super(FlinkEngineFactory.ENGINE_NAME, Type.STREAM, FLINK_CAPABILITIES);
    this.config = config;
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    Preconditions.checkArgument(plan instanceof FlinkStreamPhysicalPlan);
    FlinkStreamPhysicalPlan flinkPlan = (FlinkStreamPhysicalPlan) plan;

    return CompletableFuture.supplyAsync(()->{

      try {
        FlinkEnvironmentBuilder executablePlanVisitor = new FlinkEnvironmentBuilder(errors);
        StatementSet statementSet = flinkPlan.getExecutablePlan().accept(executablePlanVisitor, null);
        TableResult rslt = statementSet.execute();
        rslt.print(); //todo: this just forces print to wait for the async
        ExecutionResult result = new ExecutionResult.Message(rslt.getJobClient().get()
                .getJobID().toString());
        return result;
      } catch (Exception e) {
        log.error("Execution error", e);
        throw e;
      }

    });
  }

  @Override
  public FlinkStreamPhysicalPlan plan(StagePlan stagePlan,
      List<StageSink> inputs, ExecutionPipeline pipeline, SqrlFramework framework,
      TableSink errorSink, ErrorCollector errorCollector) {
    Preconditions.checkArgument(inputs.isEmpty());
    Preconditions.checkArgument(stagePlan instanceof StreamStagePlan);
    StreamStagePlan plan = (StreamStagePlan) stagePlan;
    return new FlinkPhysicalPlanner(new SqrlToFlinkExecutablePlan(errorSink,
        framework.getQueryPlanner().getRelBuilder(), errorCollector), framework.getQueryPlanner().getRelBuilder())
        .createStreamGraph(this.config,
        plan.getQueries(), errorSink, plan.getJars(), plan.getUdfs());
  }

  @Override
  public void close() throws IOException {
//    jobs.clear();
  }

}
