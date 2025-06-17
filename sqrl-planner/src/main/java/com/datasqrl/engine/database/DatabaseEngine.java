/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine.database;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.export.ExportEngine;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;

/**
 * A {@link DatabaseEngine} is a {@link ExecutionEngine} that persists data for retrieval and uses
 * indexing strategies to improve query performance. It optionally has a query engine for executing
 * queries.
 */
public interface DatabaseEngine extends ExecutionEngine, ExportEngine {

  EnginePhysicalPlan plan(MaterializationStagePlan stagePlan);

  /**
   * @return Whether this database engine supports the given query engine.
   */
  boolean supportsQueryEngine(QueryEngine engine);

  /**
   * Adds a query engine to this database engine. Throws an exception if engine is not supported.
   *
   * @param engine The query engine to add to this database engine for query execution.
   * @throws UnsupportedOperationException if the engine does not support the provided {@link
   *     QueryEngine}.
   */
  void addQueryEngine(QueryEngine engine);
}
