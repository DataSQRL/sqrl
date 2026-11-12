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
package com.datasqrl.engine.log;

import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.export.ExportEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import org.apache.calcite.rel.type.RelDataType;

public interface LogEngine extends ExportEngine, MutationEngine, DatabaseEngine {

  @Override
  default boolean supportsQueryEngine(QueryEngine engine) {
    return false;
  }

  @Override
  default void addQueryEngine(QueryEngine engine) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * Returns whether the deployment of a standalone included project shares this engine's tables
   * with the current project, like Kafka topics. The included project then owns the tables that its
   * own statements write, and the current project does not write them again.
   */
  default boolean sharesTablesWithStandaloneIncludes() {
    return false;
  }

  /**
   * Creates the table of a computed table that a standalone included project writes, so that the
   * current project can read it without writing it. The parameters are the same as for {@link
   * #createTable}. Only called if {@link #sharesTablesWithStandaloneIncludes()}.
   *
   * @param standaloneInclude the namespace of the standalone included project that writes the table
   */
  default EngineCreateTable createIncludedTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      TableAnalysis tableAnalysis,
      String standaloneInclude) {
    throw new UnsupportedOperationException("Does not share tables with standalone includes");
  }
}
