package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SnowflakeDbQuery;

public interface JdbcClient {
  ResolvedQuery prepareQuery(JdbcQuery pgQuery, Context context);

  ResolvedQuery noPrepareQuery(JdbcQuery jdbcQuery, Context context);
}
