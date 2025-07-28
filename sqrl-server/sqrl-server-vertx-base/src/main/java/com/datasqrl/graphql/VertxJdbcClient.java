/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.graphql;

import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.graphql.jdbc.JdbcClient;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.util.RequestContext;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Map;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Purpose: Manages SQL clients and executes queries. Collaboration: Used by {@link VertxContext} to
 * prepare and execute SQL queries.
 */
@Slf4j
@Value
public class VertxJdbcClient implements JdbcClient {
  Map<DatabaseType, SqlClient> clients;

  @Override
  public ResolvedQuery prepareQuery(SqlQuery query, Context context) {
    var requestId = RequestContext.getRequestId();
    log.debug("[{}] Preparing query for database: {}", requestId, query.getDatabase());

    var sqlClient = clients.get(query.getDatabase());
    if (sqlClient == null) {
      log.error("[{}] Could not find database engine: {}", requestId, query.getDatabase());
      throw new RuntimeException("Could not find database engine: " + query.getDatabase());
    }

    log.trace("[{}] Preparing SQL: {}", requestId, query.getSql());
    var preparedQuery = sqlClient.preparedQuery(query.getSql());

    return new ResolvedSqlQuery(query, new PreparedSqrlQueryImpl(preparedQuery));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context) {
    return new ResolvedSqlQuery(sqlQuery, null);
  }

  public Future<RowSet<Row>> execute(DatabaseType database, PreparedQuery query, Tuple tup) {
    var requestId = RequestContext.getRequestId();
    log.debug("[{}] Executing prepared query on database: {}", requestId, database);
    log.trace("[{}] Query parameters: {}", requestId, tup);

    var sqlClient = clients.get(database);

    if (database == DatabaseType.DUCKDB) {
      log.debug("[{}] Setting up DuckDB with Iceberg extension", requestId);
      return sqlClient
          .query("INSTALL iceberg;")
          .execute()
          .onSuccess(
              v -> {
                log.trace("[{}] DuckDB Iceberg extension installed", requestId);
              })
          .onFailure(
              err -> {
                log.warn(
                    "[{}] Failed to install DuckDB Iceberg extension: {}",
                    requestId,
                    err.getMessage());
              })
          .compose(v -> sqlClient.query("LOAD iceberg;").execute())
          .onSuccess(
              v -> {
                log.trace("[{}] DuckDB Iceberg extension loaded", requestId);
              })
          .onFailure(
              err -> {
                log.warn(
                    "[{}] Failed to load DuckDB Iceberg extension: {}",
                    requestId,
                    err.getMessage());
              })
          .compose(
              t -> {
                log.trace("[{}] Executing main query after DuckDB setup", requestId);
                return query.execute(tup);
              })
          .onSuccess(
              result -> {
                var rowSet = (RowSet<Row>) result;
                log.debug(
                    "[{}] Query executed successfully, {} rows returned", requestId, rowSet.size());
              })
          .onFailure(
              err -> {
                var throwable = (Throwable) err;
                log.error("[{}] Query execution failed: {}", requestId, throwable.getMessage());
              });
    }

    return query
        .execute(tup)
        .onSuccess(
            result -> {
              var rowSet = (RowSet<Row>) result;
              log.debug(
                  "[{}] Query executed successfully, {} rows returned", requestId, rowSet.size());
            })
        .onFailure(
            err -> {
              var throwable = (Throwable) err;
              log.error("[{}] Query execution failed: {}", requestId, throwable.getMessage());
            });
  }

  public Future<RowSet<Row>> execute(DatabaseType database, String query, Tuple tup) {
    var requestId = RequestContext.getRequestId();
    log.debug("[{}] Executing string query on database: {}", requestId, database);
    log.trace("[{}] Query SQL: {}", requestId, query);

    var sqlClient = clients.get(database);
    return execute(database, sqlClient.preparedQuery(query), tup);
  }

  @Value
  public static class PreparedSqrlQueryImpl
      implements PreparedSqrlQuery<PreparedQuery<RowSet<Row>>> {
    PreparedQuery<RowSet<Row>> preparedQuery;
  }
}
