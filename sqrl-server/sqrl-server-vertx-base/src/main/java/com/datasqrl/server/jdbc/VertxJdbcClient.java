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
package com.datasqrl.server.jdbc;

import com.datasqrl.server.ServerContext;
import com.datasqrl.server.VertxServerContext;
import com.datasqrl.server.graphql.RootGraphQLModel.ResolvedQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.ResolvedSqlQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Purpose: Manages SQL clients and executes queries. Collaboration: Used by {@link
 * VertxServerContext} to prepare and execute SQL queries. Every statement it runs is reported to
 * its {@link JdbcStatementListener}s.
 */
@Slf4j
public record VertxJdbcClient(
    Map<DatabaseType, SqlClient> clients, List<JdbcStatementListener> listeners)
    implements JdbcClient {

  public VertxJdbcClient {
    listeners = List.copyOf(listeners);
  }

  public VertxJdbcClient(Map<DatabaseType, SqlClient> clients) {
    this(clients, List.of());
  }

  @Override
  public ResolvedQuery prepareQuery(SqlQuery query, ServerContext context) {
    var sqlClient = clients.get(query.getDatabase());
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + query.getDatabase());
    }

    var preparedQuery = sqlClient.preparedQuery(query.getSql());

    return new ResolvedSqlQuery(query, new PreparedVertxSqrlQuery(preparedQuery));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, ServerContext context) {
    return new ResolvedSqlQuery(sqlQuery, null);
  }

  /**
   * Runs {@code query} and reports its rows, or its failure, to every listener before the returned
   * future completes, so before the caller can read the rows. Without listeners, the statement runs
   * untimed.
   */
  public Future<RowSet<Row>> execute(
      JdbcStatement statement, PreparedQuery<RowSet<Row>> query, Tuple params) {
    if (listeners.isEmpty()) {
      return query.execute(params);
    }

    var start = System.nanoTime();
    return query
        .execute(params)
        .andThen(
            (rows, failure) -> {
              var elapsed = Duration.ofNanos(System.nanoTime() - start);
              listeners.forEach(listener -> report(listener, statement, rows, failure, elapsed));
            });
  }

  /** Prepares {@link JdbcStatement#sql()} on the database's client and runs it. */
  public Future<RowSet<Row>> execute(JdbcStatement statement, DatabaseType database, Tuple params) {
    var sqlClient = clients.get(database);

    return execute(statement, sqlClient.preparedQuery(statement.sql()), params);
  }

  /** Isolates the listener: Vert.x would fail the statement's future if it threw. */
  private static void report(
      JdbcStatementListener listener,
      JdbcStatement statement,
      RowSet<Row> rows,
      Throwable failure,
      Duration elapsed) {
    try {
      if (failure == null) {
        listener.onRows(statement, rows, elapsed);
      } else {
        listener.onFailure(statement, failure, elapsed);
      }
    } catch (RuntimeException e) {
      log.warn(
          "Statement listener {} failed at {}",
          listener.getClass().getSimpleName(),
          statement.fieldPath(),
          e);
    }
  }
}
