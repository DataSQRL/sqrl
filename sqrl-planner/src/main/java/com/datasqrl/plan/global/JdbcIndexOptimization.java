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
package com.datasqrl.plan.global;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.relational.AbstractJDBCDatabaseEngine;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

/** Determines optimal index structure and primary key order */
@Value
@AutoService(PhysicalPlanRewriter.class)
public class JdbcIndexOptimization implements PhysicalPlanRewriter {

  @Override
  public boolean appliesTo(EnginePhysicalPlan plan) {
    return plan instanceof JdbcPhysicalPlan jpp
        && jpp.stage().engine() instanceof AbstractJDBCDatabaseEngine;
  }

  @Override
  public JdbcPhysicalPlan rewrite(EnginePhysicalPlan plan, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var jdbcPlan = (JdbcPhysicalPlan) plan;
    var engine = (AbstractJDBCDatabaseEngine) jdbcPlan.stage().engine();
    /*TODO: optimize the order of primary key columns (unless primary key is explicitly defined by hint)
       - partition keys come first
       - remaining keys are sorted based on access patterns (use indexselector)
       use CreateTableJdbcStatement#updatePrimaryKey
    */
    var indexSelectorConfig = engine.getIndexSelectorConfig();
    var indexSelector = new IndexSelector(sqrlEnv, indexSelectorConfig, jdbcPlan.tableIdMap());

    Collection<QueryIndexSummary> queryIndexSummaries =
        jdbcPlan.queries().stream()
            .map(indexSelector::getIndexSelection)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    List<IndexDefinition> indexDefinitions =
        new ArrayList<>(indexSelector.optimizeIndexes(queryIndexSummaries).keySet());
    jdbcPlan
        .tableIdMap()
        .values()
        .forEach(
            stmt -> {
              var createTable = stmt.getEngineTable();
              var tableName = createTable.tableName();
              var table = createTable.tableAnalysis();
              indexSelector
                  .getIndexHints(tableName, table)
                  .ifPresent(
                      indexHints -> {
                        // First, remove all generated indexes for that table...
                        indexDefinitions.removeIf(idx -> idx.getTableName().equals(tableName));
                        // and overwrite with the specified ones
                        indexDefinitions.addAll(indexHints);
                      });
            });
    var builder = jdbcPlan.toBuilder();
    var stmtFactory = engine.getStatementFactory();
    indexDefinitions.stream().sorted().map(stmtFactory::addIndex).forEach(builder::statement);
    return builder.build();
  }
}
