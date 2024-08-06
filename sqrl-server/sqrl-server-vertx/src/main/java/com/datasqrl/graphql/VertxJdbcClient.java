package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import lombok.Value;

@Value
public class VertxJdbcClient implements JdbcClient {
  SqlClient sqlClient;

  @Override
  public ResolvedQuery prepareQuery(JdbcQuery pgQuery, Context context) {
    PreparedQuery<RowSet<Row>> preparedQuery = sqlClient
        .preparedQuery(pgQuery.getSql());

    return new ResolvedJdbcQuery(pgQuery,
        new PreparedSqrlQueryImpl(preparedQuery));
  }

  @Value
  public static class PreparedSqrlQueryImpl
      implements PreparedSqrlQuery<PreparedQuery<RowSet<Row>>> {
    PreparedQuery<RowSet<Row>> preparedQuery;
  }
}
