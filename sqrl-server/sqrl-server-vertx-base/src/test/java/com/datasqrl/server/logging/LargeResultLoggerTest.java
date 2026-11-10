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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.server.jdbc.JdbcStatement;
import com.datasqrl.server.util.LogCapture;
import graphql.execution.ResultPath;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.time.Duration;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

class LargeResultLoggerTest {

  private static final String SQL = "SELECT *\n  FROM \"orders\"\n WHERE \"id\" = $1";

  private final LargeResultLogger logger = new LargeResultLogger(10000);

  @Test
  void givenResultBelowThreshold_whenOnRows_thenNothingIsLoggedHoweverLongItTook() {
    try (var log = LogCapture.of(LargeResultLogger.class)) {
      logger.onRows(statement("/orders"), rowsOfSize(9999), Duration.ofMinutes(1));

      assertThat(log.messages(Level.WARN)).isEmpty();
    }
  }

  @Test
  void givenResultAtThreshold_whenOnRows_thenWarnsWithPathRowsTimeAndOneLineSql() {
    try (var log = LogCapture.of(LargeResultLogger.class)) {
      logger.onRows(statement("/customers[0]/orders"), rowsOfSize(10000), Duration.ofMillis(140));

      assertThat(log.messages(Level.WARN))
          .containsExactly(
              "Large result at /customers[0]/orders: 10000 rows in 140 ms;"
                  + " sql: SELECT * FROM \"orders\" WHERE \"id\" = $1");
    }
  }

  @Test
  void givenFailedStatement_whenOnFailure_thenNothingIsLogged() {
    try (var log = LogCapture.of(LargeResultLogger.class)) {
      logger.onFailure(statement("/orders"), new IllegalStateException(), Duration.ofMillis(1));

      assertThat(log.messages(Level.WARN)).isEmpty();
      assertThat(log.messages(Level.ERROR)).isEmpty();
    }
  }

  private static JdbcStatement statement(String path) {
    return new JdbcStatement(ResultPath.parse(path), SQL);
  }

  @SuppressWarnings("unchecked")
  private static RowSet<Row> rowsOfSize(int size) {
    RowSet<Row> rows = mock(RowSet.class);
    when(rows.size()).thenReturn(size);
    return rows;
  }
}
