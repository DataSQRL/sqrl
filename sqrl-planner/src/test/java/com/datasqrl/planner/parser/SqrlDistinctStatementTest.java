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
package com.datasqrl.planner.parser;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorLocation.FileLocation;
import org.junit.jupiter.api.Test;

class SqrlDistinctStatementTest {

  private final SqrlStatementParser parser = new SqrlStatementParser();

  @Test
  void givenDistinctStatement_whenMapSqlLocation_thenPointsIntoOnOrderByAndFrom() {
    var distinct =
        (SqrlDistinctStatement)
            parser.parseStatement(
                "Latest := DISTINCT Orders ON customer_id ORDER BY updated_at DESC;");

    assertThat(distinct.mapSqlLocation(sqlLocationOf(distinct, "customer_id")))
        .isEqualTo(new FileLocation(1, 30));
    assertThat(distinct.mapSqlLocation(sqlLocationOf(distinct, "updated_at")))
        .isEqualTo(new FileLocation(1, 51));
    assertThat(distinct.mapSqlLocation(sqlLocationOf(distinct, "Orders")))
        .isEqualTo(new FileLocation(1, 20));
  }

  @Test
  void givenMultiLineDistinctStatement_whenMapSqlLocation_thenPointsIntoTheRightLine() {
    var distinct =
        (SqrlDistinctStatement)
            parser.parseStatement(
                """
                Latest := DISTINCT Orders
                  ON customer_id,
                     region
                  ORDER BY updated_at DESC;
                """);

    assertThat(distinct.mapSqlLocation(sqlLocationOf(distinct, "region")))
        .isEqualTo(new FileLocation(3, 6));
    assertThat(distinct.mapSqlLocation(sqlLocationOf(distinct, "updated_at")))
        .isEqualTo(new FileLocation(4, 12));
  }

  /** The location of a token in the SQL that the DISTINCT statement is rewritten into. */
  private static FileLocation sqlLocationOf(SqrlDistinctStatement distinct, String token) {
    var sql = distinct.getPrefix() + distinct.getDefinitionBody().get();
    return SqlScriptStatementSplitter.computeOffset(sql, sql.indexOf(token));
  }
}
