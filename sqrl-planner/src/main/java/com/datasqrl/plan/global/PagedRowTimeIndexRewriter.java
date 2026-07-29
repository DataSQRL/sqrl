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

import com.datasqrl.deployment.model.JdbcStatementModel.Type;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.database.relational.AbstractJDBCDatabaseEngine;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Adds a btree index on the rowtime column of every physical table that backs a paginated ({@code
 * OffsetPageInfo}) query. Those queries run a companion {@code MIN/MAX(rowtime)} aggregate for
 * their {@code first/lastEventTime}; the btree lets the database answer it from the index endpoints
 * instead of scanning the table.
 */
@AutoService(PhysicalPlanRewriter.class)
public class PagedRowTimeIndexRewriter implements PhysicalPlanRewriter {

  @Override
  public boolean appliesTo(EnginePhysicalPlan enginePlan) {
    return enginePlan instanceof JdbcPhysicalPlan jpp
        && jpp.stage().engine() instanceof AbstractJDBCDatabaseEngine;
  }

  @Override
  public boolean satisfied(PhysicalPlan fullPlan) {
    return getServerPlan(fullPlan).isPresent();
  }

  @Override
  public JdbcPhysicalPlan rewrite(
      PhysicalPlan fullPlan, EnginePhysicalPlan enginePlan, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var jdbcPlan = (JdbcPhysicalPlan) enginePlan;
    var serverPlan =
        getServerPlan(fullPlan)
            .orElseThrow(() -> new IllegalStateException("Server physical plan is missing"));

    var pagedTables = serverPlan.getPagedRowTimeTables();
    if (pagedTables.isEmpty()) {
      return jdbcPlan;
    }

    var engine = (AbstractJDBCDatabaseEngine) jdbcPlan.stage().engine();
    if (!engine.getIndexSelectorConfig().supportedIndexTypes().contains(IndexType.BTREE)) {
      return jdbcPlan;
    }
    var stmtFactory = engine.getStatementFactory();
    // Existing indexes (e.g. from JdbcIndexOptimization) may already cover the rowtime column.
    var existingIndexNames =
        jdbcPlan.getStatementsForType(Type.INDEX).stream()
            .map(JdbcStatement::getName)
            .collect(Collectors.toSet());

    var builder = jdbcPlan.toBuilder();
    for (var createTbl : jdbcPlan.tableIdMap().values()) {
      var engineTable = createTbl.getEngineTable();
      var tableAnalysis = engineTable.tableAnalysis();
      if (!pagedTables.contains(tableAnalysis)
          && !pagedTables.contains(tableAnalysis.getBaseTable())) {
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

  private Optional<ServerPhysicalPlan> getServerPlan(PhysicalPlan fullPlan) {
    return fullPlan.getPlans(ServerPhysicalPlan.class).findAny();
  }
}
