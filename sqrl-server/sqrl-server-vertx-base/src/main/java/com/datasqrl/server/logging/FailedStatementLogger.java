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
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

/** Logs every failed statement with the API field it ran for and its SQL. */
@Slf4j
public class FailedStatementLogger implements JdbcStatementListener {

  @Override
  public void onFailure(JdbcStatement statement, Throwable failure, Duration elapsed) {
    log.error(
        "Query failed at {}; sql: {}",
        statement.fieldPath(),
        JdbcStatementListener.formatSql(statement.sql()),
        failure);
  }
}
