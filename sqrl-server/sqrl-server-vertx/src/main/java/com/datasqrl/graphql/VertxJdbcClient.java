package com.datasqrl.graphql;

import com.datasqrl.graphql.jdbc.PreparedSqrlQueryImpl;
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
import java.util.Map;
import lombok.Value;

@Value
public class VertxJdbcClient implements JdbcClient {
  Map<String, SqlClient> clients;

  @Override
  public ResolvedQuery prepareQuery(JdbcQuery query, Context context) {
    SqlClient sqlClient = clients.get(query.getDatabase());
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + query.getDatabase());
    }

    PreparedQuery<RowSet<Row>> preparedQuery = sqlClient
        .preparedQuery(query.getSql());

    return new ResolvedJdbcQuery(query,
        new PreparedSqrlQueryImpl(preparedQuery));
  }

  @Value
  public static class PreparedSqrlQueryImpl
      implements PreparedSqrlQuery<PreparedQuery<RowSet<Row>>> {
    PreparedQuery<RowSet<Row>> preparedQuery;
  }
}
