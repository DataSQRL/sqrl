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
  void givenBlockCommentOutsideStringLiteral_whenSplitStatements_thenPreservesComment() {
    var script =
        """
        SELECT 1 /* regular block comment */ AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT 1 /* regular block comment */ AS `val`;
            """);
  }

  @Test
  void givenMultilineBlockCommentOutsideStringLiteral_whenSplitStatements_thenPreservesComment() {
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
            /* block comment with ' quote, -- line comment, and ; delimiter
               still inside the block comment
            */
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
  void givenWhitespaceBeforeHintPlus_whenSplitStatements_thenPreservesBlockComment() {
    var script =
        """
        SELECT /* + not a hint */ 1 AS `val`
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            SELECT /* + not a hint */ 1 AS `val`;
            """);
  }

  @Test
  void givenTrailingBlockComment_whenSplitStatements_thenRetainsStatementLocations() {
    var script =
        """
        SELECT 1; /* explains the first query

                   and its multiline form */
        SELECT 2;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::getFileLocation)
        .containsExactly(new FileLocation(1, 1), new FileLocation(4, 1));
  }

  @Test
  void givenApostropheInQuotedIdentifier_whenSplitStatements_thenKeepsFollowingStatements() {
    var script =
        """
        Src := SELECT source_id, patient_id FROM Records;

        Apostrophe := SELECT s.patient_id AS `it's` FROM Src AS s;

        AfterAlias := SELECT source_id FROM Records;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            "Src := SELECT source_id, patient_id FROM Records;\n",
            "Apostrophe := SELECT s.patient_id AS `it's` FROM Src AS s;\n",
            "AfterAlias := SELECT source_id FROM Records;\n");
    assertThat(statements)
        .extracting(ParsedObject::getFileLocation)
        .containsExactly(new FileLocation(1, 1), new FileLocation(3, 1), new FileLocation(5, 1));
  }

  @Test
  void
      givenCommentMarkersAndDelimiterInQuotedIdentifier_whenSplitStatements_thenPreservesIdentifier() {
    var script =
        """
        SELECT 1 AS `a--b;c`, 2 AS `x/*y`, 3 AS `it``'s`;
        SELECT 4 AS `val`;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            "SELECT 1 AS `a--b;c`, 2 AS `x/*y`, 3 AS `it``'s`;\n", "SELECT 4 AS `val`;\n");
  }

  @Test
  void givenBacktickInStringLiteral_whenSplitStatements_thenIgnoresBacktick() {
    var script =
        """
        SELECT 'it`s -- not a comment' AS `val`;
        SELECT 2 AS `val`;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly("SELECT 'it`s -- not a comment' AS `val`;\n", "SELECT 2 AS `val`;\n");
  }

  @Test
  void givenEscapedQuoteInStringLiteral_whenSplitStatements_thenPreservesLiteral() {
    var script =
        """
        SELECT 'it''s; -- not a comment' AS `val`;
        SELECT 2 AS `val`;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly("SELECT 'it''s; -- not a comment' AS `val`;\n", "SELECT 2 AS `val`;\n");
  }

  @Test
  void givenStatementDelimiterAtLineEndInStringLiteral_whenSplitStatements_thenDoesNotSplitEarly() {
    var script =
        """
        SELECT 'first line;
                last line' AS `val`
        FROM Records;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements).extracting(ParsedObject::get).containsExactly(script);
  }

  @Test
  void givenUnterminatedStringLiteral_whenSplitStatements_thenEmitsRemainderAsOneStatement() {
    var script =
        """
        SELECT 1 AS `val`;
        SELECT 'unterminated AS `val`;
        SELECT 3 AS `val`;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            "SELECT 1 AS `val`;\n", "SELECT 'unterminated AS `val`;\nSELECT 3 AS `val`;\n");
    assertThat(statements)
        .extracting(ParsedObject::getFileLocation)
        .containsExactly(new FileLocation(1, 1), new FileLocation(2, 1));
  }

  @Test
  void givenUnterminatedBlockComment_whenSplitStatements_thenEmitsRemainderAsOneStatement() {
    var script =
        """
        SELECT 1 AS `val`;
        /* unterminated comment
        SELECT 3 AS `val`;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly("SELECT 1 AS `val`;\n", "/* unterminated comment\nSELECT 3 AS `val`;\n");
    assertThat(statements)
        .extracting(ParsedObject::getFileLocation)
        .containsExactly(new FileLocation(1, 1), new FileLocation(2, 1));
  }

  @Test
  void
      givenTrailingLineCommentAfterUndelimitedLastStatement_whenSplitStatements_thenKeepsStatement() {
    var script =
        """
        SELECT 1 AS `val`;
        SELECT 2 AS `val`
        -- trailing comment
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly("SELECT 1 AS `val`;\n", "SELECT 2 AS `val`;\n");
    assertThat(statements)
        .extracting(ParsedObject::getFileLocation)
        .containsExactly(new FileLocation(1, 1), new FileLocation(2, 1));
  }

  @Test
  void givenTrailingCommentsAfterLastStatement_whenSplitStatements_thenIgnoresComments() {
    var script =
        """
        SELECT 1 AS `val`;
        /* trailing */ -- note
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements).extracting(ParsedObject::get).containsExactly("SELECT 1 AS `val`;\n");
  }

  @Test
  void givenQuotesInDoubleQuotedIdentifier_whenSplitStatements_thenKeepsFollowingStatements() {
    var script =
        """
        PassThrough RETURNS (customerid BIGINT) := SELECT customerid FROM "Customer" AS "it's";
        PassThroughBacktick RETURNS (customerid BIGINT) := SELECT customerid FROM "Customer" AS "a`b";
        AfterAlias := SELECT customerid FROM Customer;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            "PassThrough RETURNS (customerid BIGINT) := SELECT customerid FROM \"Customer\" AS \"it's\";\n",
            "PassThroughBacktick RETURNS (customerid BIGINT) := SELECT customerid FROM \"Customer\" AS \"a`b\";\n",
            "AfterAlias := SELECT customerid FROM Customer;\n");
  }

  @Test
  void givenMultilineDoubleQuotedIdentifier_whenSplitStatements_thenKeepsFollowingStatements() {
    var script =
        """
        PassThrough RETURNS (customerid BIGINT NOT NULL) :=
        SELECT customerid FROM "Customer" AS "a
        b";
        AfterAlias := SELECT customerid FROM Customer;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly(
            """
            PassThrough RETURNS (customerid BIGINT NOT NULL) :=
            SELECT customerid FROM "Customer" AS "a
            b";
            """,
            "AfterAlias := SELECT customerid FROM Customer;\n");
  }

  @Test
  void givenUnterminatedQuotedIdentifier_whenSplitStatements_thenEmitsRemainderAsOneStatement() {
    var script =
        """
        SELECT 1 AS `val`;
        SELECT 2 AS `unterminated;
        SELECT 3 AS val;
        """;

    var statements = SqlScriptStatementSplitter.splitStatements(script);

    assertThat(statements)
        .extracting(ParsedObject::get)
        .containsExactly("SELECT 1 AS `val`;\n", "SELECT 2 AS `unterminated;\nSELECT 3 AS val;\n");
    assertThat(statements)
        .extracting(ParsedObject::getFileLocation)
        .containsExactly(new FileLocation(1, 1), new FileLocation(2, 1));
  }
}
