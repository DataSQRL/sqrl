/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.engine.stream.flink.plan.FlinkTableRegistration.FlinkTableRegistrationContext;
import com.datasqrl.plan.global.OptimizedDAG;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;

@AllArgsConstructor
public class FlinkPhysicalPlanner {

  private final FlinkStreamEngine streamEngine;

  public FlinkStreamPhysicalPlan createStreamGraph(
      List<? extends OptimizedDAG.Query> streamQueries) {
    final FlinkStreamEngine.Builder streamBuilder = streamEngine.createJob();
    final StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl) streamBuilder.getTableEnvironment();

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    //TODO: push down filters across queries to determine if we can constraint sources by time for efficiency (i.e. only load the subset of the stream that is required)
    StreamStatementSet streamStatementSet = tEnv.createStatementSet();
    FlinkTableRegistration tableRegistration = new FlinkTableRegistration();
//    for (OptimizedDAG.Query q : streamQueries) {
      streamQueries.get(3).accept(tableRegistration, new FlinkTableRegistrationContext(tEnv, streamBuilder, streamStatementSet));
//    }
    return new FlinkStreamPhysicalPlan(streamStatementSet);
  }
}
