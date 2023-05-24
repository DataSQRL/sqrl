package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.KafkaMutationCoords;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import graphql.schema.DataFetcher;
import java.util.Map;
import java.util.Set;

//  @Value
public interface Context {

  JdbcClient getClient();

  DataFetcher<Object> createPropertyFetcher(String name);

  DataFetcher<?> createArgumentLookupFetcher(BuildGraphQLEngine server, Map<Set<Argument>, ResolvedQuery> lookupMap);

  DataFetcher<?> createSinkFetcher(KafkaMutationCoords coords);
}
