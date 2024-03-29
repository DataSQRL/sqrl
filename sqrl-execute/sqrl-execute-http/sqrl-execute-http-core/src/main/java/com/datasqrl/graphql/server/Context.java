package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import graphql.schema.DataFetcher;
import java.util.List;
import java.util.Map;
import java.util.Set;

//  @Value
public interface Context {

  JdbcClient getClient();

  DataFetcher<Object> createPropertyFetcher(String name);

  DataFetcher<?> createArgumentLookupFetcher(BuildGraphQLEngine server, Map<Set<Argument>, ResolvedQuery> lookupMap);

  DataFetcher<?> createSinkFetcher(MutationCoords coords);

  DataFetcher<?> createSubscriptionFetcher(SubscriptionCoords coords, Map<String, String> filters);
}
