package com.datasqrl.graphql.server;

import java.util.Set;

import com.datasqrl.graphql.jdbc.JdbcClient;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;

import graphql.schema.DataFetcher;

//  @Value

/**
 * Interface for context objects that provide database clients and data fetchers.
 */
public interface Context {

  JdbcClient getClient();

  DataFetcher<Object> createPropertyFetcher(String name);

  DataFetcher<?> createArgumentLookupFetcher(GraphQLEngineBuilder server, Set<Argument> arguments, ResolvedQuery resolvedQuery);
}
