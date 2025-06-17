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
package com.datasqrl.engine.export;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public interface ExportEngine extends ExecutionEngine {

  /**
   * Creates a table in the engine for the given execution stage. Assume that the schema/columns
   * have already been defined in the FlinkTableBuilder as well as the primary & partition key. The
   * engine should add connector options and watermarks.
   *
   * @param stage The execution stage for the engine
   * @param originalTableName The original name of the table. The actual table name might be
   *     different to make it unique.
   * @param tableBuilder The table builder
   * @param relDataType The datatype for the columns in the table.
   * @param tableAnalysis The table analysis for the table if this is a planned table (not available
   *     for mutations)
   * @return
   */
  EngineCreateTable createTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis);

  DataTypeMapping getTypeMapping();
}
