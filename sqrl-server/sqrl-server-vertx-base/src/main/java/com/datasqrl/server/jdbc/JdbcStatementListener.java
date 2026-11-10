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

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.time.Duration;

/**
 * Observes each SQL statement the server runs for an API field. Listeners run on the event loop, so
 * they must not block. A listener that throws is logged and skipped: it never fails the statement.
 *
 * <p>The elapsed time counts from issuing the statement, so it includes any wait for a pool
 * connection and for the event loop.
 */
public interface JdbcStatementListener {

  /** Longer SQL is cut by {@link #formatSql}, so that one statement cannot flood the log. */
  int MAX_LOGGED_SQL_LENGTH = 2000;

  /** Called when the rows arrive, before anything reads them, such as the conversion to JSON. */
  default void onRows(JdbcStatement statement, RowSet<Row> rows, Duration elapsed) {}

  /** Called when the statement fails. */
  default void onFailure(JdbcStatement statement, Throwable failure, Duration elapsed) {}

  /** The SQL on one line for a log line, cut after {@link #MAX_LOGGED_SQL_LENGTH} characters. */
  static String formatSql(String sql) {
    var oneLine = sql.strip().replaceAll("\\s+", " ");
    if (oneLine.length() <= MAX_LOGGED_SQL_LENGTH) {
      return oneLine;
    }

    return "%s... (%d chars)"
        .formatted(oneLine.substring(0, MAX_LOGGED_SQL_LENGTH), oneLine.length());
  }
}
