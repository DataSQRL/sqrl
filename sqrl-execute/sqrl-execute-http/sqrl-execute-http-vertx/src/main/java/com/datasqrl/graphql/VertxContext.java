package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.GraphQLArgumentWrapper;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import graphql.schema.DataFetcher;
import io.vertx.ext.web.handler.graphql.schema.VertxDataFetcher;
import io.vertx.ext.web.handler.graphql.schema.VertxPropertyDataFetcher;
import java.util.Map;
import java.util.Set;
import lombok.Value;

@Value
public class VertxContext implements Context {

  VertxJdbcClient sqlClient;

  @Override
  public JdbcClient getClient() {
    return sqlClient;
  }

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return VertxPropertyDataFetcher.create(name);
  }

  @Override
  public DataFetcher<?> createArgumentLookupFetcher(BuildGraphQLEngine server, Map<Set<Argument>, ResolvedQuery> lookupMap) {
    return VertxDataFetcher.create((env, fut) -> {
      //Map args
      Set<FixedArgument> argumentSet = GraphQLArgumentWrapper.wrap(env.getArguments())
          .accept(server, lookupMap);

      //Find query
      ResolvedQuery resolvedQuery = lookupMap.get(argumentSet);
      if (resolvedQuery == null) {
        throw new RuntimeException("Could not find query");
      }

      //Execute
      QueryExecutionContext context = new VertxQueryExecutionContext(this,
          env, argumentSet, fut);
      resolvedQuery.accept(server, context);
    });
  }
}
