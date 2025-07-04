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
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public interface LogEngine extends ExportEngine, DatabaseEngine {

  default EngineCreateTable createMutation(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      MutationInsertType insertType) {
    throw new UnsupportedOperationException("Does not support mutations");
  }

  @Override
  default boolean supportsQueryEngine(QueryEngine engine) {
    return false;
  }

  @Override
  default void addQueryEngine(QueryEngine engine) {
    throw new UnsupportedOperationException("Not supported");
  }
}
