package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.BuildGraphQLEngine;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.graphql.server.Model.VariableArgument;
import com.datasqrl.graphql.server.QueryExecutionContext;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLArgument;
import graphql.schema.PropertyDataFetcher;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Value;

@Value
public class JdbcContext implements Context {

  GenericJdbcClient client;

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return PropertyDataFetcher.fetching(name);
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(BuildGraphQLEngine server,
      Map<Set<Argument>, ResolvedQuery> lookupMap) {

    //Runtime execution, keep this as light as possible
    return (env) -> {

      //Map args
      Set<Argument> argumentSet = new HashSet<>();
      for (GraphQLArgument argument : env.getFieldDefinition().getArguments()) {
        VariableArgument arg = new VariableArgument(argument.getName(),
            env.getArguments().get(argument.getName()));
        argumentSet.add(arg);
      }

      //Find query
      ResolvedQuery resolvedQuery = lookupMap.get(argumentSet);
      if (resolvedQuery == null) {
        throw new RuntimeException("Could not find query");
      }

      //Execute
      QueryExecutionContext context = new JdbcExecutionContext(this,
          env, argumentSet);
      CompletableFuture future = resolvedQuery.accept(server, context);
      return future;
    };
  }

  @Override
  public DataFetcher<?> createSinkFetcher(MutationCoords coords) {
    throw new RuntimeException("Mutations not yet supported");
  }

  @Override
  public DataFetcher<?> createSubscriptionFetcher(SubscriptionCoords coords,
      Map<String, String> filters) {
    throw new RuntimeException("Subscriptions not yet supported");
  }
}
