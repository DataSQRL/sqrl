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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class JdbcStatementListenerTest {

  @Test
  void givenMultiLineSql_whenFormatSql_thenCollapsesItToOneLine() {
    assertThat(
            JdbcStatementListener.formatSql("  SELECT *\n  FROM \"orders\"\n WHERE \"id\" = $1\n"))
        .isEqualTo("SELECT * FROM \"orders\" WHERE \"id\" = $1");
  }

  @Test
  void givenSqlLongerThanLimit_whenFormatSql_thenCutsItAndNamesTheFullLength() {
    var sql = "SELECT " + "x".repeat(JdbcStatementListener.MAX_LOGGED_SQL_LENGTH);

    assertThat(JdbcStatementListener.formatSql(sql))
        .hasSize(JdbcStatementListener.MAX_LOGGED_SQL_LENGTH + "... (2007 chars)".length())
        .startsWith("SELECT xxx")
        .endsWith("x... (2007 chars)");
  }
}
