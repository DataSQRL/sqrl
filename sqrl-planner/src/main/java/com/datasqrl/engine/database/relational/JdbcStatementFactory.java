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
package com.datasqrl.engine.database.relational;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import java.util.List;
import java.util.Map;
import lombok.Value;

public interface JdbcStatementFactory {

  JdbcDialect getDialect();

  JdbcStatement createTable(JdbcEngineCreateTable createTable);

  default List<JdbcStatement> extractExtensions(List<Query> queries) {
    return List.of();
  }

  default boolean supportsQueries() {
    return true;
  }

  QueryResult createQuery(
      Query query, boolean withView, Map<String, JdbcEngineCreateTable> tableIdMap);

  JdbcStatement addIndex(IndexDefinition indexDefinition);

  @Value
  class QueryResult {
    ExecutableJdbcReadQuery.ExecutableJdbcReadQueryBuilder execQueryBuilder;
    JdbcStatement view;
  }
}
