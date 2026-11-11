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

import com.datasqrl.server.jdbc.JdbcStatement;
import com.datasqrl.server.util.LogCapture;
import graphql.execution.ResultPath;
import io.vertx.sqlclient.RowSet;
import java.time.Duration;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

class FailedStatementLoggerTest {

  private static final JdbcStatement STATEMENT =
      new JdbcStatement(
          ResultPath.parse("/orders"), "SELECT *\n  FROM \"orders\"\n WHERE \"id\" = $1");

  private final FailedStatementLogger logger = new FailedStatementLogger();

  @Test
  void givenFailedStatement_whenOnFailure_thenLogsPathSqlAndFailure() {
    var failure = new IllegalStateException("relation \"orders\" does not exist");

    try (var log = LogCapture.of(FailedStatementLogger.class)) {
      logger.onFailure(STATEMENT, failure, Duration.ofMillis(3));

      assertThat(log.events(Level.ERROR))
          .singleElement()
          .satisfies(
              event -> {
                assertThat(event.getMessage().getFormattedMessage())
                    .isEqualTo(
                        "Query failed at /orders; sql: SELECT * FROM \"orders\" WHERE \"id\" = $1");
                assertThat(event.getThrown()).isSameAs(failure);
              });
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void givenRows_whenOnRows_thenNothingIsLogged() {
    try (var log = LogCapture.of(FailedStatementLogger.class)) {
      logger.onRows(STATEMENT, mock(RowSet.class), Duration.ofMinutes(1));

      assertThat(log.messages(Level.ERROR)).isEmpty();
    }
  }
}
