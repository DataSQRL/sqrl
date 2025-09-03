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
package com.datasqrl.planner.dag.plan;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.tables.SqrlTableFunction;
import java.util.List;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.rel.RelNode;

/**
 * During DAG planning, we use this stage plan to keep track of all exports and imports to this
 * execution stage plus any queries we run against it.
 */
@Value
@SuperBuilder
public class MaterializationStagePlan {

  ExecutionStage stage;

  /** All the sinks for that stage which are tables we export to in Flink */
  @Singular List<EngineCreateTable> tables;

  /**
   * All the queries that this stage executes against the data, only applies to databse/log stages
   */
  @Singular List<Query> queries;

  /** All the mutations we write to this stage, only applies to logs */
  @Singular List<EngineCreateTable> mutations;

  /** Passed through since the engines might need it for query manipulation */
  Utils utils;

  public record Query(SqrlTableFunction function, RelNode relNode, ErrorCollector errors) {}

  public record Utils(SqrlRexUtil rexUtil) {}
}
