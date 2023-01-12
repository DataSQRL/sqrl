/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkTableRegistration.FlinkTableRegistrationContext;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.global.OptimizedDAG.ExternalSink;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;

@AllArgsConstructor
public class FlinkPhysicalPlanner {

  private final FlinkStreamEngine streamEngine;

  public FlinkStreamPhysicalPlan createStreamGraph(
      List<? extends OptimizedDAG.Query> streamQueries, TableSink errorSink) {
    final FlinkStreamEngine.Builder streamBuilder = streamEngine.createJob();
    final FlinkTableRegistrationContext regContext = streamBuilder.getContext();

    regContext.getTEnv().getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    //TODO: push down filters across queries to determine if we can constraint sources by time for efficiency (i.e. only load the subset of the stream that is required)
    FlinkTableRegistration tableRegistration = new FlinkTableRegistration();
    for (OptimizedDAG.Query q : streamQueries) {
      q.accept(tableRegistration, regContext);
    }
    streamBuilder.getErrorStream().ifPresent(errorStream -> tableRegistration.registerErrors(
        errorStream, new ExternalSink("_errors", errorSink), regContext));
    return new FlinkStreamPhysicalPlan(regContext.getStreamStatementSet());
  }
}
