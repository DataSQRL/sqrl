/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import static com.datasqrl.engine.EngineCapability.CUSTOM_FUNCTIONS;
import static com.datasqrl.engine.EngineCapability.DATA_MONITORING;
import static com.datasqrl.engine.EngineCapability.DENORMALIZE;
import static com.datasqrl.engine.EngineCapability.EXTENDED_FUNCTIONS;
import static com.datasqrl.engine.EngineCapability.TEMPORAL_JOIN;
import static com.datasqrl.engine.EngineCapability.TIME_WINDOW_AGGREGATION;

import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkPhysicalPlanner;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.OptimizedDAG;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

public abstract class AbstractFlinkStreamEngine extends ExecutionEngine.Base implements
    StreamEngine {

  public static final EnumSet<EngineCapability> FLINK_CAPABILITIES = EnumSet.of(DENORMALIZE,
      TEMPORAL_JOIN,
      TIME_WINDOW_AGGREGATION, EXTENDED_FUNCTIONS, CUSTOM_FUNCTIONS, DATA_MONITORING);

  final FlinkEngineConfiguration config;

  public AbstractFlinkStreamEngine(FlinkEngineConfiguration config) {
    super(FlinkEngineConfiguration.ENGINE_NAME, Type.STREAM, FLINK_CAPABILITIES);
    this.config = config;
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan) {
    Preconditions.checkArgument(plan instanceof FlinkStreamPhysicalPlan);
    FlinkStreamPhysicalPlan flinkPlan = (FlinkStreamPhysicalPlan) plan;
    StatementSet statementSet = flinkPlan.getStatementSet();
    TableResult rslt = statementSet.execute();
    rslt.print(); //todo: this just forces print to wait for the async
    return new ExecutionResult.Message(rslt.getJobClient().get()
        .getJobID().toString());
  }

  @Override
  public FlinkStreamPhysicalPlan plan(OptimizedDAG.StagePlan plan,
      List<OptimizedDAG.StageSink> inputs, RelBuilder relBuilder, TableSink errorSink) {
    Preconditions.checkArgument(inputs.isEmpty());
    FlinkStreamPhysicalPlan streamPlan = new FlinkPhysicalPlanner(this).createStreamGraph(
        plan.getQueries(), errorSink);
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
