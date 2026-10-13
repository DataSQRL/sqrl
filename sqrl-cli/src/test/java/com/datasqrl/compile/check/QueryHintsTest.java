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
package com.datasqrl.compile.check;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.compile.check.QueryHints.Problem;
import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.junit.jupiter.api.Test;

class QueryHintsTest {

  private static final SqlParser.Config PARSER =
      SqlParser.config()
          .withParserFactory(FlinkSqlParserImpl.FACTORY)
          .withConformance(FlinkSqlConformance.DEFAULT)
          .withLex(Lex.JAVA);

  @Test
  void givenJoinHintsThatNameInputs_whenProblems_thenFindsNone() throws SqlParseException {
    assertThat(
            problems(
                """
                SELECT /*+ BROADCAST(c), shuffle_hash(D, c) */ d.*
                FROM Dedup AS d JOIN Parts AS c ON d.id = c.id
                """))
        .isEmpty();
    // Also with the name of an aliased table, and in a temporal join
    assertThat(
            problems(
                """
                SELECT /*+ NEST_LOOP(Parts), STATE_TTL('d' = '1d', 'c' = '12h') */ d.*
                FROM Dedup d JOIN Parts FOR SYSTEM_TIME AS OF d.ts AS c ON d.id = c.id
                """))
        .isEmpty();
  }

  @Test
  void givenHintsThatNameNoInput_whenProblems_thenFindsTheirOptions() throws SqlParseException {
    var problems =
        problems(
            """
            SELECT /*+ BROADCAST(zz), STATE_TTL('x' = '1d'), LOOKUP('table' = 'nope') */ d.*
            FROM Dedup AS d JOIN Parts AS c ON d.id = c.id
            """);

    assertThat(problems)
        .extracting(problem -> problem.position().getColumnNum())
        .containsExactly(22, 37);
    assertThat(problems.get(0).message())
        .isEqualTo(
            "Hint BROADCAST names `zz`, which is not a table or alias of this query, so Flink"
                + " rejects it. The query has: `d`, `c`.");
    assertThat(problems.get(1).message()).startsWith("Hint STATE_TTL names `x`");
  }

  @Test
  void givenHintInSubquery_whenProblems_thenChecksTheInputsOfItsQuery() throws SqlParseException {
    var problems =
        problems(
            """
            SELECT p.* FROM (
              SELECT /*+ BROADCAST(p) */ a.id FROM A AS a JOIN B AS b ON a.id = b.id
            ) AS p
            """);

    assertThat(problems).singleElement().satisfies(p -> assertThat(p.message()).contains("`p`"));
  }

  private static List<Problem> problems(String sql) throws SqlParseException {
    return QueryHints.problems(SqlParser.create(sql, PARSER).parseQuery());
  }
}
