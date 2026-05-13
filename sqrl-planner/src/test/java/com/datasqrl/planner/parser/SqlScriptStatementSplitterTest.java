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
  void givenDocCommentMarkerInStringLiteral_whenSplitStatements_thenPreservesLiteral() {
    var script =
        """
        SELECT 'first line;
                /** doc comment
                    multi line
                */
                last line' AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT 'first line;
                    /** doc comment
                        multi line
                    */
                    last line' AS `val`;
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

  @Test
  void givenSingleQuoteInSingleLineDocComment_whenSplitStatements_thenIgnoresQuote() {
    var script =
        """
        /** Dummy one liner comment. '' This -- is still part of the doc' comment. */
        SELECT 'hello -- not a comment' AS `val`;-- comment
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            /** Dummy one liner comment. '' This -- is still part of the doc' comment. */
            SELECT 'hello -- not a comment' AS `val`;
            """);
  }

  @Test
  void givenSingleQuoteInMultiLineDocComment_whenSplitStatements_thenIgnoresQuote() {
    var script =
        """
        /** Dummy multi line comment. ''
            This -- is still part of the doc' comment. */
        SELECT 'hello -- not a comment' AS `val`;-- comment
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            /** Dummy multi line comment. ''
                This -- is still part of the doc' comment. */
            SELECT 'hello -- not a comment' AS `val`;
            """);
  }

  @Test
  void givenBlockCommentInStringLiteral_whenSplitStatements_thenPreservesLiteral() {
    var script =
        """
        SELECT 'first line;
                /* block comment
                    multi line
                */
                last line' AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT 'first line;
                    /* block comment
                        multi line
                    */
                    last line' AS `val`;
            """);
  }

  @Test
  void givenBlockCommentOutsideStringLiteral_whenSplitStatements_thenRemovesComment() {
    var script =
        """
        SELECT 1 /* regular block comment */ AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT 1  AS `val`;
            """);
  }

  @Test
  void givenMultilineBlockCommentOutsideStringLiteral_whenSplitStatements_thenRemovesComment() {
    var script =
        """
        SELECT 1 AS `before`
        /* block comment with ' quote, -- line comment, and ; delimiter
           still inside the block comment
        */
        , 2 AS `after`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT 1 AS `before`
            , 2 AS `after`;
            """);
  }

  @Test
  void givenSqlHintComment_whenSplitStatements_thenPreservesHint() {
    var script =
        """
        SELECT /*+ OPTIONS('key'='value') */ 1 AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT /*+ OPTIONS('key'='value') */ 1 AS `val`;
            """);
  }

  @Test
  void givenWhitespaceBeforeHintPlus_whenSplitStatements_thenRemovesBlockComment() {
    var script =
        """
        SELECT /* + not a hint */ 1 AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT  1 AS `val`;
            """);
  }
}
