package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.RootGraphqlModel.DuckDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Map;
import lombok.Value;

@Value
public class VertxJdbcClient implements JdbcClient {
  Map<String, SqlClient> clients;

  @Override
  public ResolvedQuery prepareQuery(JdbcQuery query, Context context) {
    String database = query instanceof DuckDbQuery ? "duckdb" : "postgres";

    SqlClient sqlClient = clients.get(database);
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + database);
    }

    PreparedQuery<RowSet<Row>> preparedQuery = sqlClient
        .preparedQuery(query.getSql());

    return new ResolvedJdbcQuery(query,
        new PreparedSqrlQueryImpl(preparedQuery));
  }

  public Future<RowSet<Row>> execute(String database, PreparedQuery query, Tuple tup) {
    if (database == null) {
      database = "postgres";
    }
    SqlClient sqlClient = clients.get(database);

    if (database.equalsIgnoreCase("duckdb")) {
      return sqlClient.query("INSTALL iceberg;").execute().compose(v ->
          sqlClient.query("LOAD iceberg;").execute()).compose(t->query.execute(tup));
    }
    return query.execute(tup);
  }

  public Future<RowSet<Row>> execute(String database, String query, Tuple tup) {
    SqlClient sqlClient = clients.get(database);
    return execute(database, sqlClient.preparedQuery(query), tup);
  }

  @Value
  public static class PreparedSqrlQueryImpl
      implements PreparedSqrlQuery<PreparedQuery<RowSet<Row>>> {
    PreparedQuery<RowSet<Row>> preparedQuery;
  }
}
