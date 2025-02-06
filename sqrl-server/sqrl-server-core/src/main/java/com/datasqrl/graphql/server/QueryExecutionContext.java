package com.datasqrl.graphql.server;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedPagedJdbcQuery;

import graphql.schema.DataFetchingEnvironment;

public interface QueryExecutionContext {

  Context getContext();
  DataFetchingEnvironment getEnvironment();
  Set<Argument> getArguments();
  CompletableFuture runQuery(GraphQLEngineBuilder graphQLEngineBuilder, ResolvedJdbcQuery pgQuery, boolean isList);
  CompletableFuture runPagedJdbcQuery(ResolvedPagedJdbcQuery pgQuery,
      boolean isList, QueryExecutionContext context);
}
