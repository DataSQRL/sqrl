package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoordsVisitor;

public interface SubscriptionConfiguration<R> {
  SubscriptionCoordsVisitor<R, Context> createSubscriptionFetcherVisitor();
}
