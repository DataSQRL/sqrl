package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;

public interface JdbcClient {
  ResolvedQuery prepareQuery(JdbcQuery pgQuery, Context context);
}
