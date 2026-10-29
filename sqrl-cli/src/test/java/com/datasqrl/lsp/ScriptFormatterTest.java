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
package com.datasqrl.lsp;

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.lsp4j.FormattingOptions;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.junit.jupiter.api.Test;

class ScriptFormatterTest {

  private static String format(String script) {
    return ScriptFormatter.format(script, "    ").orElseThrow();
  }

  @Test
  void givenQuery_whenFormatted_thenIndentsClauseBodiesAndConditions() {
    var script =
        """
        _Actions := SELECT
          id,
              COALESCE(flag, FALSE)
          AS flag
          FROM Events e
            LEFT JOIN Config c
          ON e.id = c.id
          AND c.enabled
            WHERE e.kind IN (
          'A',
          'B'
          )
          AND e.amount > 0
        GROUP BY id, flag;
        """;

    assertThat(format(script))
        .isEqualTo(
            """
            _Actions := SELECT
                id,
                COALESCE(flag, FALSE)
                    AS flag
            FROM Events e
            LEFT JOIN Config c
                ON e.id = c.id
                    AND c.enabled
            WHERE e.kind IN (
                    'A',
                    'B'
                )
                AND e.amount > 0
            GROUP BY id, flag;
            """);
  }

  @Test
  void givenSubqueryAndCase_whenFormatted_thenIndentsThemUnderTheirLine() {
    var script =
        """
        Status := SELECT
        CASE
        WHEN status = 5 THEN 'FAILED'
        ELSE 'OK'
        END AS label
        FROM (
        SELECT status
        FROM Updates
        )
        WHERE label IS NOT NULL;
        """;

    assertThat(format(script))
        .isEqualTo(
            """
            Status := SELECT
                CASE
                    WHEN status = 5 THEN 'FAILED'
                    ELSE 'OK'
                END AS label
            FROM (
                SELECT status
                FROM Updates
            )
            WHERE label IS NOT NULL;
            """);
  }

  @Test
  void givenTableDefinitionsAndFunctions_whenFormatted_thenIndentsTheirParentheses() {
    var script =
        """
        CREATE TABLE Orders (
          id BIGINT NOT NULL,
          tags ARRAY < STRING >,
          event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
        ) WITH (
        'connector' = 'datagen'
        );
        OrdersOf (
        id BIGINT NOT NULL,
        tenant STRING NOT NULL METADATA FROM 'auth.tenant'
        )
          := SELECT * FROM Orders
        WHERE id = :id;
        Latest := DISTINCT Orders
          ON id
        ORDER BY event_time DESC;
        """;

    assertThat(format(script))
        .isEqualTo(
            """
            CREATE TABLE Orders (
                id BIGINT NOT NULL,
                tags ARRAY<STRING>,
                event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
            ) WITH (
                'connector' = 'datagen'
            );
            OrdersOf (
                id BIGINT NOT NULL,
                tenant STRING NOT NULL METADATA FROM 'auth.tenant'
            )
            := SELECT * FROM Orders
            WHERE id = :id;
            Latest := DISTINCT Orders
            ON id
            ORDER BY event_time DESC;
            """);
  }

  @Test
  void givenTokensWithoutSpaces_whenFormatted_thenSpacesOperatorsAndCommas() {
    var script =
        "X:=SELECT t.*,COUNT(*) AS n,-1 AS m,a+b*2 AS s,f( x ,y ) FROM t WHERE a>=-2 AND b<>c;\n";

    assertThat(format(script))
        .isEqualTo(
            "X := SELECT t.*, COUNT(*) AS n, -1 AS m, a + b * 2 AS s, f(x, y) FROM t"
                + " WHERE a >= -2 AND b <> c;\n");
  }

  @Test
  void givenNamedArguments_whenFormatted_thenSpacesArrows() {
    assertThat(format("X := SELECT * FROM TABLE(fn(id=>'a',  ids=>ARRAY['b']));\n"))
        .isEqualTo("X := SELECT * FROM TABLE(fn(id => 'a', ids => ARRAY['b']));\n");
  }

  @Test
  void givenCommentsLiteralsAndNames_whenFormatted_thenKeepsThemAsWritten() {
    var script =
        """
        -- Comment   with  spaces
        /*+no_query,  primary_key(id) */
        MixedCase := SELECT `my  column`, 'a  b' AS s, "x  y" -- trailing   note
          FROM   Events;
        """;

    assertThat(format(script))
        .isEqualTo(
            """
            -- Comment   with  spaces
            /*+no_query,  primary_key(id) */
            MixedCase := SELECT `my  column`, 'a  b' AS s, "x  y" -- trailing   note
            FROM Events;
            """);
  }

  @Test
  void givenCommentInQuery_whenFormatted_thenIndentsItLikeTheNextLine() {
    var script =
        """
        X := SELECT
        -- the id
        id
        FROM t;
        """;

    assertThat(format(script))
        .isEqualTo(
            """
            X := SELECT
                -- the id
                id
            FROM t;
            """);
  }

  @Test
  void givenImportsAndTemplates_whenFormatted_thenKeepsThemAsWritten() {
    var script =
        """
        IMPORT data-sources.*;
        IMPORT sources-{{variant}}.Orders  AS o;
        {{#withTests}}
          /*+test */
          T := SELECT * FROM {{table}}_x;
        {{/withTests}}
        """;

    assertThat(format(script))
        .isEqualTo(
            """
            IMPORT data-sources.*;
            IMPORT sources-{{variant}}.Orders  AS o;
            {{#withTests}}
            /*+test */
            T := SELECT * FROM {{table}}_x;
            {{/withTests}}
            """);
  }

  @Test
  void givenBlankLinesAndTrailingSpaces_whenFormatted_thenTrimsThem() {
    assertThat(format("\n\nX := SELECT 1;   \n\n\nY := SELECT 2;\n\n\n"))
        .isEqualTo("X := SELECT 1;\n\n\nY := SELECT 2;\n");
  }

  @Test
  void givenWindowsLineBreaks_whenFormatted_thenKeepsThem() {
    assertThat(format("X := SELECT\r\nid\r\nFROM t;"))
        .isEqualTo("X := SELECT\r\n    id\r\nFROM t;\r\n");
  }

  @Test
  void givenUnterminatedString_whenFormatted_thenLeavesScriptUnchanged() {
    assertThat(ScriptFormatter.format("X := SELECT 'open\n", "    ")).isEmpty();
  }

  @Test
  void givenFormattedScript_whenEdits_thenReturnsNone() {
    var script = "X := SELECT\n    id\nFROM t;\n";

    assertThat(ScriptFormatter.edits(script, new FormattingOptions(4, true))).isEmpty();
  }

  @Test
  void givenUnformattedScript_whenEditsWithTabs_thenReplacesWholeScript() {
    var edits = ScriptFormatter.edits("X := SELECT\nid\nFROM t;", new FormattingOptions(4, false));

    assertThat(edits).hasSize(1);
    assertThat(edits.get(0).getRange())
        .isEqualTo(new Range(new Position(0, 0), new Position(2, 7)));
    assertThat(edits.get(0).getNewText()).isEqualTo("X := SELECT\n\tid\nFROM t;\n");
  }
}
