/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.EngineCapability.STANDARD_STREAM;

import com.datasqrl.FlinkEnvironmentBuilder;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkPhysicalPlanner;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

public abstract class AbstractFlinkStreamEngine extends ExecutionEngine.Base implements
    StreamEngine {

  public static final EnumSet<EngineCapability> FLINK_CAPABILITIES = STANDARD_STREAM;
  final ExecutionEnvironmentFactory execFactory;

  public AbstractFlinkStreamEngine(ExecutionEnvironmentFactory execFactory, SqrlConfig config) {
    super(FlinkEngineFactory.ENGINE_NAME, Type.STREAM, FLINK_CAPABILITIES);
    this.execFactory = execFactory;
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    Preconditions.checkArgument(plan instanceof FlinkStreamPhysicalPlan);
    FlinkStreamPhysicalPlan flinkPlan = (FlinkStreamPhysicalPlan) plan;

    return CompletableFuture.supplyAsync(()->{
      FlinkEnvironmentBuilder executablePlanVisitor = new FlinkEnvironmentBuilder(errors);
      StatementSet statementSet = flinkPlan.getExecutablePlan().accept(executablePlanVisitor, null);

      TableResult rslt = statementSet.execute();
      rslt.print(); //todo: this just forces print to wait for the async
      ExecutionResult result = new ExecutionResult.Message(rslt.getJobClient().get()
          .getJobID().toString());
      return result;
    });
  }

  @Override
  public FlinkStreamPhysicalPlan plan(PhysicalDAGPlan.StagePlan stagePlan,
      List<PhysicalDAGPlan.StageSink> inputs,
      ExecutionPipeline pipeline, RelBuilder relBuilder, TableSink errorSink) {
    Preconditions.checkArgument(inputs.isEmpty());
    Preconditions.checkArgument(stagePlan instanceof StreamStagePlan);
    StreamStagePlan plan = (StreamStagePlan) stagePlan;
    return new FlinkPhysicalPlanner(relBuilder).createStreamGraph(
        plan.getQueries(), errorSink, plan.getJars(), plan.getUdfs());
  }

  public abstract FlinkStreamBuilder createJob();

  @Override
  public DataMonitor createDataMonitor() {
    return createJob();
  }

  @Override
  public void close() throws IOException {
//    jobs.clear();
  }

}
