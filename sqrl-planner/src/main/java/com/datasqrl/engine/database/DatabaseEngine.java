/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.export.ExportEngine;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;

/**
 * A {@link DatabaseEngine} is a {@link ExecutionEngine} that persists data for retrieval and uses
 * indexing strategies to improve query performance. It optionally has a query engine for executing queries.
 */
public interface DatabaseEngine extends ExecutionEngine, ExportEngine {

  EnginePhysicalPlan plan(MaterializationStagePlan stagePlan);

  /**
   * @return The {@link IndexSelectorConfig} for the engine that is used to determine which indexes to use
   * by the optimizer.
   */
  @Deprecated
  IndexSelectorConfig getIndexSelectorConfig();

  /**
   *
   * @return Whether this database engine supports the given query engine.
   */
  boolean supportsQueryEngine(QueryEngine engine);

  /**
   * Adds a query engine to this database engine. Throws an exception if engine is not supported.
   *
   *
   * @param engine The query engine to add to this database engine for query execution.
   * @throws UnsupportedOperationException if the engine does not support the provided {@link QueryEngine}.
   */
  void addQueryEngine(QueryEngine engine);

}
