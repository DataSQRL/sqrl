package com.datasqrl.graphql.jdbc;

import java.util.Set;
import java.util.stream.Collectors;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.VariableArgument;

import graphql.schema.DataFetcher;
import graphql.schema.PropertyDataFetcher;
import lombok.Value;

@Value
public class JdbcContext implements Context {

  GenericJdbcClient client;

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return PropertyDataFetcher.fetching(name);
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(GraphQLEngineBuilder server,
      Set<Argument> arguments, ResolvedQuery resolvedQuery) {
    //Runtime execution, keep this as light as possible
    return (env) -> {
      Set<Argument> argumentSet =
          env.getArguments().entrySet().stream()
              .map(argument -> new VariableArgument(argument.getKey(), argument.getValue()))
              .collect(Collectors.toSet());

      QueryExecutionContext context = new JdbcExecutionContext(this,
          env, argumentSet);
      return resolvedQuery.accept(server, context);
    };
  }

}
