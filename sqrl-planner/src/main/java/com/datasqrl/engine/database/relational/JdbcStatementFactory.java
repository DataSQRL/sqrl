package com.datasqrl.engine.database.relational;

import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;
import java.util.List;
import lombok.Value;

public interface JdbcStatementFactory {

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
