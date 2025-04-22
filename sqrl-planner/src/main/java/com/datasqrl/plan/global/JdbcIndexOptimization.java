package com.datasqrl.plan.global;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.relational.AbstractJDBCDatabaseEngine;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.v2.Sqrl2FlinkSQLTranslator;
import com.google.auto.service.AutoService;

import lombok.Value;

@Value
@AutoService(PhysicalPlanRewriter.class)
public class JdbcIndexOptimization implements PhysicalPlanRewriter {


  @Override
  public boolean appliesTo(EnginePhysicalPlan plan) {
    return plan instanceof JdbcPhysicalPlan jpp &&
        ((JdbcPhysicalPlan) plan).getStage().getEngine() instanceof AbstractJDBCDatabaseEngine;
  }

  @Override
  public JdbcPhysicalPlan rewrite(EnginePhysicalPlan plan, Sqrl2FlinkSQLTranslator sqrlEnv) {
    var jdbcPlan = (JdbcPhysicalPlan) plan;
    var engine = (AbstractJDBCDatabaseEngine) jdbcPlan.getStage().getEngine();
    var indexSelectorConfig = engine.getIndexSelectorConfig();
    var indexSelector = new IndexSelector(sqrlEnv, indexSelectorConfig, jdbcPlan.getTableMap());

    Collection<QueryIndexSummary> queryIndexSummaries = jdbcPlan.getQueries().stream().map(indexSelector::getIndexSelection)
        .flatMap(List::stream).collect(Collectors.toList());
    List<IndexDefinition> indexDefinitions = new ArrayList<>(indexSelector.optimizeIndexes(queryIndexSummaries)
        .keySet());
    jdbcPlan.getTableMap().forEach((tableName, table) -> indexSelector.getIndexHints(tableName, table).ifPresent(indexHints -> {
      //First, remove all generated indexes for that table...
      indexDefinitions.removeIf(idx -> idx.getTableId().equals(tableName));
      //and overwrite with the specified ones
      indexDefinitions.addAll(indexHints);
    }));
    var builder = jdbcPlan.toBuilder();
    var stmtFactory = engine.getStatementFactory();
    indexDefinitions.stream().sorted()
        .map(stmtFactory::addIndex)
        .forEach(builder::statement);
    return builder.build();
  }

}
