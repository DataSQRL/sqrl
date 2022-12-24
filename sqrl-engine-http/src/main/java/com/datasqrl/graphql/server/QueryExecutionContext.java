package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.ResolvedPagedPgQuery;
import com.datasqrl.graphql.server.Model.ResolvedPgQuery;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface QueryExecutionContext {

  Context getContext();
  DataFetchingEnvironment getEnvironment();
  Set<FixedArgument> getArguments();
  CompletableFuture runQuery(SqrlGraphQLServer sqrlGraphQLServer, ResolvedPgQuery pgQuery, boolean isList);
  CompletableFuture runPagedQuery(ResolvedPagedPgQuery pgQuery,
      boolean isList);
}
