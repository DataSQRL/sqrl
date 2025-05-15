package com.datasqrl.graphql.server;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;

import graphql.schema.DataFetchingEnvironment;

public interface QueryExecutionContext {

  Context getContext();
  DataFetchingEnvironment getEnvironment();
  Set<Argument> getArguments();
  CompletableFuture runQuery(ResolvedSqlQuery pgQuery, boolean isList);

}
