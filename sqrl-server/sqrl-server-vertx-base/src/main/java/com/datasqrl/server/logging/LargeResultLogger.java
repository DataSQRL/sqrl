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
package com.datasqrl.server.logging;

import com.datasqrl.server.jdbc.JdbcStatement;
import com.datasqrl.server.jdbc.JdbcStatementListener;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Warns about a statement that returns at least {@code rowCountThreshold} rows. Converting such a
 * result to JSON can block the event loop until the pod is killed, so the warning is written as
 * soon as the rows arrive.
 */
@Slf4j
@RequiredArgsConstructor
public class LargeResultLogger implements JdbcStatementListener {

  private final long rowCountThreshold;

  @Override
  public void onRows(JdbcStatement statement, RowSet<Row> rows, Duration elapsed) {
    if (rows.size() >= rowCountThreshold) {
      log.warn(
          "Large result at {}: {} rows in {} ms; sql: {}",
          statement.fieldPath(),
          rows.size(),
          elapsed.toMillis(),
          JdbcStatementListener.formatSql(statement.sql()));
    }
  }
}
