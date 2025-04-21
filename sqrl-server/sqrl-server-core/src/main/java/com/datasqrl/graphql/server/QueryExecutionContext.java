package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface QueryExecutionContext {

  Context getContext();
  DataFetchingEnvironment getEnvironment();
  Set<Argument> getArguments();
  CompletableFuture runQuery(ResolvedSqlQuery pgQuery, boolean isList);

}
