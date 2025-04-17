package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoordsVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresLogMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoordsVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.VariableArgument;
import com.datasqrl.graphql.server.QueryExecutionContext;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLArgument;
import graphql.schema.PropertyDataFetcher;
import java.util.HashSet;
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
  public DataFetcher<?> createArgumentLookupFetcher(GraphQLEngineBuilder server,
      Set<Argument> arguments, ResolvedQuery resolvedQuery) {
    //Runtime execution, keep this as light as possible
    return (env) -> {
      QueryExecutionContext context = new JdbcExecutionContext(this,
          env, arguments);
      CompletableFuture future = resolvedQuery.accept(server, context);
      return future;
    };
  }

}
