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

import static com.datasqrl.compile.check.CheckTestProject.MAIN_SCRIPT;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.compile.check.CheckRequest;
import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.compile.check.ScriptChecker;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionItemKind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Completes the test project with the catalog and symbols of a check. */
class ScriptCompletionTest {

  /** With a column whose name is a reserved word of SQL */
  private static final String STAMPED =
      "Stamped := SELECT id, updated_at AS `timestamp` FROM OrderStream;";

  private static final String SCRIPT =
      MAIN_SCRIPT
          + "/*+test */\nOrderTest := SELECT COUNT(*) AS num FROM Orders;\n"
          + STAMPED
          + "\n";

  @TempDir static Path tempDir;

  private static CheckTestProject project;
  private static CheckResult result;

  @BeforeAll
  static void check() throws Exception {
    project = CheckTestProject.create(tempDir);
    result =
        new ScriptChecker()
            .check(
                CheckRequest.of(project.projectRoot(), tempDir.resolve("work"))
                    .withOverlays(Map.of(project.mainScript(), SCRIPT))
                    .withKeepCatalog(true));
    assertThat(result.hasErrors()).isFalse();
    assertThat(result.catalog()).isPresent();
  }

  @AfterAll
  static void close() {
    result.catalog().ifPresent(catalog -> catalog.close());
  }

  /** Completes a statement that is added to the script, at its {@code ^}. */
  private static List<CompletionItem> complete(String statement) {
    var text = SCRIPT + statement.replace("^", "");
    var context = CompletionContext.at(text, SCRIPT.length() + statement.indexOf('^'));
    return new ScriptCompletion(
            project.mainScript(),
            result.catalog(),
            List.of(result.symbols()),
            Optional.of(tempDir.resolve("work").resolve("build")))
        .complete(context);
  }

  private static List<String> labels(List<CompletionItem> items, CompletionItemKind kind) {
    return items.stream().filter(i -> i.getKind() == kind).map(CompletionItem::getLabel).toList();
  }

  @Test
  void givenFromClause_whenComplete_thenSuggestsTablesButNotTestTables() {
    var items = complete("X := SELECT * FROM ^");

    assertThat(labels(items, CompletionItemKind.Class))
        .contains("OrderStream", "Orders", "CustomerTotals")
        .doesNotContain("OrderTest");
    assertThat(labels(items, CompletionItemKind.Module)).contains("sources");
    assertThat(labels(items, CompletionItemKind.Function)).isEmpty();
  }

  @Test
  void givenTestStatement_whenComplete_thenSuggestsTestTables() {
    var items = complete("/*+test */\nY := SELECT * FROM ^");

    assertThat(labels(items, CompletionItemKind.Class)).contains("OrderTest");
  }

  @Test
  void givenAlias_whenComplete_thenSuggestsItsColumnsWithTypes() {
    var items = complete("X := SELECT o.^ FROM Orders o");

    assertThat(items)
        .allSatisfy(item -> assertThat(item.getKind()).isEqualTo(CompletionItemKind.Field));
    assertThat(labels(items, CompletionItemKind.Field)).contains("id", "customer_id", "amount");
    assertThat(items)
        .filteredOn(item -> item.getLabel().equals("id"))
        .singleElement()
        .satisfies(item -> assertThat(item.getDetail()).startsWith("BIGINT"));
  }

  @Test
  void givenExpression_whenComplete_thenSuggestsColumnsAndFunctions() {
    var items = complete("X := SELECT * FROM Orders WHERE amount > 10 AND ^");

    assertThat(labels(items, CompletionItemKind.Field)).contains("amount", "customer_id");
    assertThat(labels(items, CompletionItemKind.Function)).contains("COALESCE", "CHAR_LENGTH");
  }

  @Test
  void givenGroupBy_whenComplete_thenSuggestsTheColumnsOfTheQuery() {
    var items = complete("X := SELECT customer_id, COUNT(*) AS n FROM Orders GROUP BY ^");

    assertThat(labels(items, CompletionItemKind.Field)).contains("customer_id", "amount");
    assertThat(labels(items, CompletionItemKind.Class)).isEmpty();
  }

  @Test
  void givenTableInFromClause_whenComplete_thenSuggestsKeywordsButNoColumns() {
    var items = complete("X := SELECT * FROM Orders ^");

    assertThat(labels(items, CompletionItemKind.Field)).isEmpty();
    assertThat(labels(items, CompletionItemKind.Keyword)).contains("JOIN", "WHERE");
  }

  @Test
  void givenDistinct_whenComplete_thenSuggestsTheColumnsOfItsTable() {
    assertThat(labels(complete("X := DISTINCT OrderStream ON ^"), CompletionItemKind.Field))
        .contains("id", "updated_at");
  }

  @Test
  void givenImport_whenComplete_thenSuggestsPackagesScriptsAndTables() {
    assertThat(labels(complete("IMPORT ^"), CompletionItemKind.Module)).containsExactly("shop");
    assertThat(labels(complete("IMPORT shop.^"), CompletionItemKind.Module))
        .containsExactly("sources");
    assertThat(complete("IMPORT shop.sources.^"))
        .extracting(CompletionItem::getLabel)
        .containsExactly("Orders", "*");
  }

  @Test
  void givenHintOrExport_whenComplete_thenSuggestsHintNamesAndSinks() {
    assertThat(labels(complete("/*+ ^ */\nY := SELECT 1;"), CompletionItemKind.Keyword))
        .contains("primary_key", "test", "query_by_all");
    assertThat(labels(complete("EXPORT Orders TO ^"), CompletionItemKind.Module))
        .contains("print", "logger", "log");
  }

  @Test
  void givenReservedColumnName_whenComplete_thenQueriesQuoteItButHintsDoNot() {
    var query = complete("X := SELECT * FROM Stamped WHERE ^");
    var hint = complete("/*+primary_key(^) */\n" + STAMPED);

    assertThat(insertText(query, "timestamp")).isEqualTo("`timestamp`");
    // The compiler reads the arguments of hints as written
    assertThat(insertText(hint, "timestamp")).isEqualTo("timestamp");
  }

  private static String insertText(List<CompletionItem> items, String label) {
    var item = items.stream().filter(i -> i.getLabel().equals(label)).findFirst().orElseThrow();
    return item.getInsertText() == null ? item.getLabel() : item.getInsertText();
  }

  @Test
  void givenColumnsOfHint_whenComplete_thenSuggestsTheColumnsOfTheTable() {
    var items = complete("/*+query_by_all(^) */\nOrders := SELECT * FROM OrderStream;");

    assertThat(labels(items, CompletionItemKind.Field)).contains("id", "customer_id", "amount");
  }
}
