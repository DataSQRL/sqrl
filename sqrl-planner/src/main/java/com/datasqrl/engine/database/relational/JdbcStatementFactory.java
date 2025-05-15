package com.datasqrl.engine.database.relational;

import com.datasqrl.config.JdbcDialect;
import java.util.List;

import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;

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

  QueryResult createQuery(Query query, boolean withView);

  JdbcStatement addIndex(IndexDefinition indexDefinition);

  @Value
  class QueryResult {
    ExecutableJdbcReadQuery.ExecutableJdbcReadQueryBuilder execQueryBuilder;
    JdbcStatement view;
  }

}
