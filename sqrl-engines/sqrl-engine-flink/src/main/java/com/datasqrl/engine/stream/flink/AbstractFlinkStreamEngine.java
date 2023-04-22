/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.EngineCapability.STANDARD_STREAM;

import com.datasqrl.FlinkEnvironmentBuilder;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkPhysicalPlanner;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

public abstract class AbstractFlinkStreamEngine extends ExecutionEngine.Base implements
    StreamEngine {

  public static final EnumSet<EngineCapability> FLINK_CAPABILITIES = STANDARD_STREAM;
  final ExecutionEnvironmentFactory execFactory;

  public AbstractFlinkStreamEngine(ExecutionEnvironmentFactory execFactory) {
    super(FlinkEngineFactory.ENGINE_NAME, Type.STREAM, FLINK_CAPABILITIES);
    this.execFactory = execFactory;
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    Preconditions.checkArgument(plan instanceof FlinkStreamPhysicalPlan);
    FlinkStreamPhysicalPlan flinkPlan = (FlinkStreamPhysicalPlan) plan;
    FlinkEnvironmentBuilder executablePlanVisitor = new FlinkEnvironmentBuilder(errors);
    StatementSet statementSet = flinkPlan.getExecutablePlan().accept(executablePlanVisitor, null);
    TableResult rslt = statementSet.execute();
    rslt.print(); //todo: this just forces print to wait for the async
    return new ExecutionResult.Message(rslt.getJobClient().get()
        .getJobID().toString());
  }

  @Override
  public FlinkStreamPhysicalPlan plan(PhysicalDAGPlan.StagePlan plan,
      List<PhysicalDAGPlan.StageSink> inputs, RelBuilder relBuilder, TableSink errorSink) {
    Preconditions.checkArgument(inputs.isEmpty());
    FlinkStreamPhysicalPlan streamPlan = new FlinkPhysicalPlanner(relBuilder).createStreamGraph(
        plan.getQueries(), errorSink, plan.getJars(), plan.getUdfs());
    return streamPlan;
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
