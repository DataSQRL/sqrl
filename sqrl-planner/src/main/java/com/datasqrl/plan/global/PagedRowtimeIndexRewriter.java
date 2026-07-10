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
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;

/**
 * Adds a btree index on the rowtime column of every physical table that backs a paginated ({@code
 * OffsetPageInfo}) query. Those queries run a companion {@code MIN/MAX(rowtime)} aggregate for
 * their {@code first/lastEventTime}; the btree lets the database answer it from the index endpoints
 * instead of scanning the table.
 *
 * <p>Unlike {@link JdbcIndexOptimization} this rewriter is constructed with the set of paginated
 * base tables (only known after the GraphQL schema walk) and is applied in a second pass.
 */
@RequiredArgsConstructor
public class PagedRowtimeIndexRewriter implements PhysicalPlanRewriter {

  private final Set<TableAnalysis> pagedBaseTables;

  @Override
  public boolean appliesTo(EnginePhysicalPlan plan) {
    return !pagedBaseTables.isEmpty()
        && plan instanceof JdbcPhysicalPlan jpp
        && jpp.stage().engine() instanceof AbstractJDBCDatabaseEngine;
  }

  @Override
  public JdbcPhysicalPlan rewrite(EnginePhysicalPlan plan, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var jdbcPlan = (JdbcPhysicalPlan) plan;
    var engine = (AbstractJDBCDatabaseEngine) jdbcPlan.stage().engine();
    if (!engine.getIndexSelectorConfig().supportedIndexTypes().contains(IndexType.BTREE)) {
      return jdbcPlan;
    }
    var stmtFactory = engine.getStatementFactory();
    // Existing indexes (e.g. from JdbcIndexOptimization) may already cover the rowtime column.
    var existingIndexNames =
        jdbcPlan.getStatementsForType(JdbcStatement.Type.INDEX).stream()
            .map(JdbcStatement::getName)
            .collect(Collectors.toSet());

    var builder = jdbcPlan.toBuilder();
    for (var createTbl : jdbcPlan.tableIdMap().values()) {
      var engineTable = createTbl.getEngineTable();
      var tableAnalysis = engineTable.tableAnalysis();
      if (!isPaged(tableAnalysis)) {
        continue;
      }
      var rowTime = tableAnalysis.getRowTime();
      if (rowTime.isEmpty()) {
        continue;
      }
      var index =
          new IndexDefinition(
              engineTable.tableName(),
              List.of(rowTime.get()),
              tableAnalysis.getRowType().getFieldNames(),
              -1,
              IndexType.BTREE);
      if (existingIndexNames.add(index.getName())) {
        builder.statement(stmtFactory.addIndex(index));
      }
    }
    return builder.build();
  }

  private boolean isPaged(TableAnalysis tableAnalysis) {
    return pagedBaseTables.contains(tableAnalysis)
        || pagedBaseTables.contains(tableAnalysis.getBaseTable());
  }
}
