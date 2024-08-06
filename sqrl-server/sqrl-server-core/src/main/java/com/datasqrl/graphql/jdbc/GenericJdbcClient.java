package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import java.sql.Connection;
import lombok.Value;

@Value
public class GenericJdbcClient implements JdbcClient {

  Connection connection;

  @Override
  public ResolvedQuery prepareQuery(JdbcQuery pgQuery, Context context) {
    return new ResolvedJdbcQuery(pgQuery,
        new PreparedSqrlQueryImpl(connection, pgQuery.getSql()));
  }
}
