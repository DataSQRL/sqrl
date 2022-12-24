package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.Model.PgQuery;
import com.datasqrl.graphql.server.Model.ResolvedPgQuery;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import java.sql.Connection;
import lombok.Value;

@Value
public class GenericJdbcClient implements JdbcClient {

  Connection connection;
  @Override
  public ResolvedQuery prepareQuery(PgQuery pgQuery, Context context) {
    GenericJdbcClient client = (GenericJdbcClient) context.getClient();

    return new ResolvedPgQuery(pgQuery,
        new PreparedSqrlQueryImpl(client.getConnection(), pgQuery.getSql()));
  }
}
