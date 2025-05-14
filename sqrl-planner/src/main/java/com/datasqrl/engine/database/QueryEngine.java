package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;

/**
 * A {@link QueryEngine} executes queries against a {@link DatabaseEngine} that supports the query
 * engine. A query engine does not persist data but only processes data stored elsewhere to produce
 * query results.
 */
public interface QueryEngine extends ExecutionEngine {

  EnginePhysicalPlan plan(MaterializationStagePlan stagePlan);

}
