package com.datasqrl.graphql;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.RootGraphqlModel.DuckDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedDuckDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedSnowflakeDbQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryBase;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SnowflakeDbQuery;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Map;
import lombok.Value;
import net.snowflake.client.jdbc.internal.google.api.Page;

/**
 * Purpose: Manages SQL clients and executes queries.
 * Collaboration: Used by {@link VertxContext} to prepare and execute SQL queries.
 */
@Value
public class VertxJdbcClient implements JdbcClient {
  Map<String, SqlClient> clients;

  @Override
  public ResolvedQuery prepareQuery(JdbcQuery query, Context context) {
    String database = getDatabaseName(query);

    SqlClient sqlClient = clients.get(database);
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + database);
    }

    PreparedQuery<RowSet<Row>> preparedQuery = sqlClient
        .preparedQuery(query.getSql());

    return new ResolvedJdbcQuery(query,
        new PreparedSqrlQueryImpl(preparedQuery));
  }

  //Todo Fix me
  public static String getDatabaseName(QueryBase query) {
    if (query instanceof DuckDbQuery || query instanceof PagedDuckDbQuery) {
      return "duckdb";
    } else if (query instanceof SnowflakeDbQuery || query instanceof PagedSnowflakeDbQuery) {
      return "snowflake";
    } else if (query instanceof JdbcQuery || query instanceof PagedJdbcQuery) {
      return "postgres";
    }
    throw new RuntimeException("Unknown database type");
  }

  @Override
  public ResolvedQuery noPrepareQuery(JdbcQuery jdbcQuery, Context context) {
    return new ResolvedJdbcQuery(jdbcQuery, null);
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
