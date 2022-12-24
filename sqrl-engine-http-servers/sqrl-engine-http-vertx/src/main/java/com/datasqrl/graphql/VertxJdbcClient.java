package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.Model.PgQuery;
import com.datasqrl.graphql.server.Model.PreparedSqrlQuery;
import com.datasqrl.graphql.server.Model.ResolvedPgQuery;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import lombok.Value;

@Value
public class VertxJdbcClient implements JdbcClient {
  SqlClient sqlClient;

  @Override
  public ResolvedQuery prepareQuery(PgQuery pgQuery, Context context) {
    PreparedQuery<RowSet<Row>> preparedQuery = sqlClient
        .preparedQuery(pgQuery.getSql());

    return new ResolvedPgQuery(pgQuery,
        new PreparedSqrlQueryImpl(preparedQuery));
  }

  @Value
  public static class PreparedSqrlQueryImpl
      implements PreparedSqrlQuery<PreparedQuery<RowSet<Row>>> {
    PreparedQuery<RowSet<Row>> preparedQuery;
  }
}
