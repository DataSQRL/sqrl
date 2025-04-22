package com.datasqrl.graphql.jdbc;

import java.sql.Connection;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;

import lombok.Value;

@Value
public class GenericJdbcClient implements JdbcClient {

  Connection connection;

  @Override
  public ResolvedQuery prepareQuery(SqlQuery pgQuery, Context context) {
    return new ResolvedSqlQuery(pgQuery,
        new PreparedSqrlQueryImpl(connection, pgQuery.getSql()));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context) {
    throw new RuntimeException("Not yet implemented");
  }
}
