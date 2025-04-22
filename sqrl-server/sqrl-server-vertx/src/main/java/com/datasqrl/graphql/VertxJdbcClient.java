package com.datasqrl.graphql;

import java.util.Map;

import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.jdbc.JdbcClient;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;

import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import lombok.Value;

/**
 * Purpose: Manages SQL clients and executes queries.
 * Collaboration: Used by {@link VertxContext} to prepare and execute SQL queries.
 */
@Value
public class VertxJdbcClient implements JdbcClient {
  Map<DatabaseType, SqlClient> clients;

  @Override
  public ResolvedQuery prepareQuery(SqlQuery query, Context context) {
    var sqlClient = clients.get(query.getDatabase());
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + query.getDatabase());
    }

    var preparedQuery = sqlClient
        .preparedQuery(query.getSql());

    return new ResolvedSqlQuery(query,
        new PreparedSqrlQueryImpl(preparedQuery));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context) {
    return new ResolvedSqlQuery(sqlQuery, null);
  }

  public Future<RowSet<Row>> execute(DatabaseType database, PreparedQuery query, Tuple tup) {
    var sqlClient = clients.get(database);

    if (database==DatabaseType.DUCKDB) {
      return sqlClient.query("INSTALL iceberg;").execute().compose(v ->
          sqlClient.query("LOAD iceberg;").execute()).compose(t->query.execute(tup));
    }
    return query.execute(tup);
  }

  public Future<RowSet<Row>> execute(DatabaseType database, String query, Tuple tup) {
    var sqlClient = clients.get(database);
    return execute(database, sqlClient.preparedQuery(query), tup);
  }

  @Value
  public static class PreparedSqrlQueryImpl
      implements PreparedSqrlQuery<PreparedQuery<RowSet<Row>>> {
    PreparedQuery<RowSet<Row>> preparedQuery;
  }
}
