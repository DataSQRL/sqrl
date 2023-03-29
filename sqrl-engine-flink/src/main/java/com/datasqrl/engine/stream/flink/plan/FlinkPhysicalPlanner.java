/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.flink.AbstractFlinkStreamEngine;
import com.datasqrl.engine.stream.flink.FlinkStreamBuilder;
import com.datasqrl.engine.stream.flink.plan.FlinkTableRegistration.FlinkTableRegistrationContext;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.Query;
import java.net.URL;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;

@AllArgsConstructor
public class FlinkPhysicalPlanner {

  private final AbstractFlinkStreamEngine streamEngine;

  public FlinkStreamPhysicalPlan createStreamGraph(
      List<? extends Query> streamQueries, TableSink errorSink, Set<URL> jars) {
    final FlinkStreamBuilder streamBuilder = streamEngine.createJob();
    final FlinkTableRegistrationContext regContext = streamBuilder.getContext();

    regContext.getTEnv().getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    //TODO: push down filters across queries to determine if we can constraint sources by time for efficiency (i.e. only load the subset of the stream that is required)
    FlinkTableRegistration tableRegistration = new FlinkTableRegistration();
    for (PhysicalDAGPlan.Query q : streamQueries) {
      q.accept(tableRegistration, regContext);
    }
    streamBuilder.getErrorHandler().getErrorStream().ifPresent(errorStream -> tableRegistration.registerErrors(
        errorStream, new ExternalSink("_errors", errorSink), regContext));
    return new FlinkStreamPhysicalPlan(regContext.getStreamStatementSet(), jars);
  }
}
