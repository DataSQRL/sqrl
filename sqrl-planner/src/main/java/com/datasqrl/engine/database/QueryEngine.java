package com.datasqrl.engine.database;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import java.util.List;

/**
 * A {@link QueryEngine} executes queries against a {@link DatabaseEngine} that supports the query
 * engine. A query engine does not persist data but only processes data stored elsewhere to produce
 * query results.
 */
public interface QueryEngine extends ExecutionEngine {

  EnginePhysicalPlan plan(MaterializationStagePlan stagePlan);


  @Override
  @Deprecated
  default DatabasePhysicalPlanOld plan(StagePlan plan, List<StageSink> inputs,
      ExecutionPipeline pipeline, List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {
    throw new UnsupportedOperationException("Query Engine planning should be invoked through TableFormatEngine via the other plan method");
  }

  @Deprecated
  DatabasePhysicalPlanOld plan(ConnectorFactoryFactory tableConnectorFactory, EngineConfig tableConnectorConfig,
      StagePlan plan, List<StageSink> inputs,
      ExecutionPipeline pipeline, List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector);

}
