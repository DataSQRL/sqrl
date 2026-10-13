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

import com.datasqrl.lsp.CompletionContext.Kind;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CompletionContextTest {

  private static final String PREVIOUS = "Orders := SELECT * FROM Stream;\n";

  /** The context at the {@code ^} in a statement after another statement. */
  private static CompletionContext at(String statement) {
    var text = PREVIOUS + statement.replace("^", "");
    return CompletionContext.at(text, PREVIOUS.length() + statement.indexOf('^'));
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "^ | STATEMENT | ''",
        "IMP^ | STATEMENT | IMP",
        "IMPORT shop.sou^ | IMPORT | sou",
        "EXPORT Ord^ | TABLE | Ord",
        "EXPORT Orders T^ | KEYWORD | T",
        "EXPORT Orders TO print.^ | EXPORT_SINK | ''",
        "/*+ primary_k^ */ | HINT | primary_k",
        "/*+ primary_key(i^) */ | NONE | ''",
        "/*+ primary_key(i^) */ X := SELECT 1; | HINT_COLUMN | i",
        "/*+ index(^) */ X := SELECT 1; | KEYWORD | ''",
        "/*+ index(hash, ^) */ X := SELECT 1; | HINT_COLUMN | ''",
        "/*+ test, engine(po^) */ | KEYWORD | po",
        "/*+ ttl(^) */ | NONE | ''",
        "IMPORT connectors.police_{{var^ | TEMPLATE | var",
        "X := SELECT * FROM T WHERE c = '{{ kafka.to^ | TEMPLATE | kafka.to",
        "-- {{^ | TEMPLATE | ''",
        "IMPORT connectors.police_{{variant}}^ | NONE | ''",
        "-- Ord^ | NONE | ''",
        "X := SELECT 'Ord^' FROM Orders; | NONE | ''",
        "X := ^ | KEYWORD | ''",
        "X := DISTINCT Ord^ | TABLE | Ord",
        "X := DISTINCT Orders ^ | KEYWORD | ''",
        "X := DISTINCT Orders ON id ORDER BY up^ | COLUMN | up",
        "Orders.total := amo^ | COLUMN | amo",
        "Orders.items := SELECT * FROM Items i WHERE i.order_id = this.i^ | COLUMN | i",
        "X(id BIGINT, name STRING) := SELECT * FROM Orders WHERE id = :^ | PARAMETER | ''",
        "X := SELECT * FROM Ord^ | QUERY | Ord",
        "CREATE TABLE T (i^ | NONE | ''",
      })
  void givenPosition_whenAt_thenFindsWhatToComplete(String statement, Kind kind, String prefix) {
    var context = at(statement);

    assertThat(context.kind()).isEqualTo(kind);
    assertThat(context.prefix()).isEqualTo(prefix);
  }

  @Test
  void givenPathOfImport_whenAt_thenSplitsItIntoQualifierAndPrefix() {
    var context = at("IMPORT `shop`.sources.Ord^");

    assertThat(context.path()).containsExactly("shop", "sources");
    assertThat(context.prefix()).isEqualTo("Ord");
  }

  @Test
  void givenQuery_whenAt_thenPassesTheBodyWithoutParametersAndKeywordsToTheCompiler() {
    var context = at("X(id BIGINT) := SUBSCRIBE SELECT * FROM Orders o WHERE o.id = :id AND o.^;");

    assertThat(context.kind()).isEqualTo(Kind.QUERY);
    assertThat(context.sql())
        .isEqualTo("           SELECT * FROM Orders o WHERE o.id = 0   AND o.");
    assertThat(context.sqlCursor()).isEqualTo(context.sql().length());
  }

  @Test
  void givenComputedParameter_whenAt_thenSuggestsTheParameters() {
    var context =
        at(
            "X(a BIGINT, b STRING, m AS MOD(a, 2), s AS CONCAT(LEFT(b, 3), '%')) := "
                + "SELECT * FROM Orders WHERE id = :^");

    assertThat(context.kind()).isEqualTo(Kind.PARAMETER);
    assertThat(context.names()).containsExactly("a", "b", "m", "s");
  }

  @Test
  void givenColumnExpression_whenAt_thenCompletesItAsAQueryOfTheTable() {
    var context = at("Orders.total := price * qua^;");

    assertThat(context.kind()).isEqualTo(Kind.QUERY);
    assertThat(context.sql()).isEqualTo("SELECT  price * qua\nFROM Orders");
    assertThat(context.sql().substring(0, context.sqlCursor())).isEqualTo("SELECT  price * qua");
  }

  @Test
  void givenStatementAboveWithoutSemicolon_whenAt_thenOnlyUsesTheCurrentStatement() {
    var context =
        CompletionContext.at(
            "A := SELECT * FROM T\nB := SELECT * FROM U u WHERE u.",
            "A := SELECT * FROM T\nB := SELECT * FROM U u WHERE u.".length());

    assertThat(context.kind()).isEqualTo(Kind.QUERY);
    assertThat(context.sql()).isEqualTo(" SELECT * FROM U u WHERE u.");
  }

  @Test
  void givenTestHint_whenAt_thenMarksTheStatementAsTest() {
    assertThat(at("/*+test */\nT := SELECT * FROM ^").testStatement()).isTrue();
    assertThat(at("T := SELECT * FROM ^").testStatement()).isFalse();
  }

  @Test
  void givenColumnsOfHint_whenAt_thenNamesTheTableOfTheNextStatement() {
    assertThat(at("/*+query_by_all(^) */\n/** Doc */\nOrders(id BIGINT) := SELECT 1;").table())
        .isEqualTo("Orders");
    assertThat(at("/*+query_by_any(a, ^) */ IMPORT shop.sources.Orders AS Src;").table())
        .isEqualTo("Src");
    assertThat(at("/*+partition_key(^) */ CREATE TABLE IF NOT EXISTS Events (id INT);").table())
        .isEqualTo("Events");
    assertThat(at("/*+index(^) */ X := SELECT 1;").names())
        .contains("hash", "btree", "vector_cosine");
    assertThat(at("/*+engine(^) */ X := SELECT 1;").names()).contains("postgres", "flink");
  }

  @Test
  void givenHintOfFlinkInQuery_whenAt_thenSuggestsTheHintsOfFlinkAndTheInputs() {
    var afterSelect = at("X := SELECT /*+ BROAD^ */ d.* FROM D AS d;");
    assertThat(afterSelect.kind()).isEqualTo(Kind.KEYWORD);
    assertThat(afterSelect.prefix()).isEqualTo("BROAD");
    assertThat(afterSelect.names())
        .contains("BROADCAST", "SHUFFLE_HASH", "STATE_TTL")
        .doesNotContain("OPTIONS", "primary_key");
    assertThat(at("X := SELECT * FROM D /*+ OPT^ */;").names()).containsExactly("OPTIONS");

    var inputs =
        at(
            "X := SELECT /*+ BROADCAST(d, ^) */ d.* FROM DedupData AS d\n"
                + "  JOIN `_Parts` c ON d.id = c.id JOIN Other ON true;");
    assertThat(inputs.kind()).isEqualTo(Kind.NAME);
    assertThat(inputs.names()).containsExactly("d", "c", "Other");
    // Its options are strings, and LOOKUP names its options
    assertThat(at("X := SELECT /*+ STATE_TTL(^) */ * FROM D AS d;").kind()).isEqualTo(Kind.NONE);
    assertThat(at("X := SELECT /*+ LOOKUP(^) */ * FROM D AS d;").kind()).isEqualTo(Kind.NONE);
  }

  @Test
  void givenDistinctColumns_whenAt_thenNamesTheTable() {
    var context = at("X := DISTINCT sources.Orders ON ^");

    assertThat(context.table()).isEqualTo("sources.Orders");
    assertThat(context.names()).isEqualTo(List.of());
  }
}
