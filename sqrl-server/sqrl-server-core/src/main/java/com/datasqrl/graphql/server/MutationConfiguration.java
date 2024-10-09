package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoordsVisitor;

public interface MutationConfiguration<R> {
  MutationCoordsVisitor<R, Context> createSinkFetcherVisitor();
}
