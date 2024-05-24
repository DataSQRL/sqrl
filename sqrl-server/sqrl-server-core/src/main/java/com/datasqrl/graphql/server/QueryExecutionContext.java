package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedPagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface QueryExecutionContext<R> {

  Context getContext();
  DataFetchingEnvironment getEnvironment();
  Set<Argument> getArguments();
  Object runQuery(GraphQLEngineBuilder graphQLEngineBuilder, ResolvedJdbcQuery pgQuery, boolean isList);
  Object runPagedJdbcQuery(ResolvedPagedJdbcQuery pgQuery,
      boolean isList, QueryExecutionContext context);
}
