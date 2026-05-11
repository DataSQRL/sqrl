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

import org.junit.jupiter.api.Test;

class SqlScriptStatementSplitterTest {

  @Test
  void givenLineCommentMarkerInStringLiteral_whenSplitStatements_thenPreservesLiteral() {
    var script =
        """
        SELECT SUBSTR('--', 1, 1) AS `val1`,
        'hello' AS `val2`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT SUBSTR('--', 1, 1) AS `val1`,
            'hello' AS `val2`;
            """);
  }

  @Test
  void givenLineCommentOutsideStringLiteral_whenSplitStatements_thenRemovesComment() {
    var script =
        """
        SELECT 'hello -- not a comment' AS `val1`;-- comment
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT 'hello -- not a comment' AS `val1`;
            """);
  }

  @Test
  void givenStatementDelimiterInMultilineStringLiteral_whenSplitStatements_thenPreservesLiteral() {
    var script =
        """
        SELECT 'first line;
                -- not a comment
                last line' AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT 'first line;
                    -- not a comment
                    last line' AS `val`;
            """);
  }
}
