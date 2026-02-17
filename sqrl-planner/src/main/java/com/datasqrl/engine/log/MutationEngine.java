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

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.graphql.server.MutationInsertType;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.time.Duration;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public interface MutationEngine extends ExecutionEngine {

  /**
   * Create a new mutation table in the project
   *
   * @param stage
   * @param originalTableName
   * @param tableBuilder
   * @param relDataType
   * @param insertType
   * @param ttl
   * @return
   */
  default MutationCreateTable createMutation(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      MutationInsertType insertType,
      Optional<Duration> ttl) {
    throw new UnsupportedOperationException("Does not support mutations");
  }

  interface MutationCreateTable extends EngineCreateTable {
    MutationCreateTable withValueType(RelDataType inputValueType);
  }
}
