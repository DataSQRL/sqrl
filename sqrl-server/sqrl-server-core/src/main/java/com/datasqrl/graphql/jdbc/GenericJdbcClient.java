package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import java.sql.Connection;
import lombok.Value;

@Value
public class GenericJdbcClient implements JdbcClient {

  Connection connection;

  @Override
  public ResolvedQuery prepareQuery(SqlQuery pgQuery, Context context) {
    return new ResolvedJdbcQuery(pgQuery,
        new PreparedSqrlQueryImpl(connection, pgQuery.getSql()));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context) {
    throw new RuntimeException("Not yet implemented");
  }
}
