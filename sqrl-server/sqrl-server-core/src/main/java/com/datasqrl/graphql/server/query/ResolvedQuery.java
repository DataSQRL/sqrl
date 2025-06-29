package com.datasqrl.graphql.server.query;

import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryBase;

public interface ResolvedQuery {

  QueryBase getQuery();

  ResolvedQuery preprocess(QueryExecutionContext context);

  public <R, C> R accept(ResolvedQueryVisitor<R, C> visitor, C context);

  interface PreparedQueryContainer<T> {

    T getPreparedQuery();
  }

  interface Preprocessor {

    ResolvedSqlQuery preprocess(ResolvedSqlQuery resolvedSqlQuery, QueryExecutionContext context);
  }

  interface ResolvedQueryVisitor<R, C> {

    R visitResolvedSqlQuery(ResolvedSqlQuery query, C context);
  }
}
