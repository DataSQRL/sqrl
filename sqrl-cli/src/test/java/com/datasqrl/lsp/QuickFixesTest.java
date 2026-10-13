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

import com.datasqrl.compile.check.CheckRequest;
import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.compile.check.ScriptChecker;
import com.datasqrl.compile.check.ScriptText;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.lsp4j.CodeAction;
import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.DiagnosticSeverity;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.TextEdit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Fixes the errors of a check of the test project, with a statement for each kind of error. */
class QuickFixesTest {

  private static final String URI = "file:///project/main.sqrl";

  private static final String SCRIPT =
      """
      IMPORT shop.sources;

      OrderStream := SELECT * FROM sources.Orders
      UnknownTable := SELECT * FROM OrderStrem;
      OtherCase := SELECT * FROM orderstream;
      UnknownColumn := SELECT amout FROM OrderStream;
      UnknownQualifiedColumn := SELECT o.custmer_id FROM OrderStream o;
      NotImported := SELECT * FROM BigOrders;
      DatabaseNotImported := SELECT * FROM helpers.BigOrders;
      /*+ primay_key(id) */
      MisspelledHint := SELECT * FROM OrderStream;
      SortColumn := SELECT id FROM OrderStream ORDER BY amount;
      SortColumns := SELECT o.id, SUM(o.amount) AS total
                     FROM OrderStream o JOIN OrderStream p ON o.id = p.id
                     GROUP BY o.id, p.updated_at, o.customer_id
                     ORDER BY p.updated_at DESC, o.customer_id;
      UnknownTableOfDatabase := SELECT * FROM sources.Ordrs;
      TableOfPackage := SELECT * FROM Orders;
      InSubquery := SELECT * FROM OrderStream WHERE id IN (SELECT id FROM OrdrStream);
      OrderStrem.others := SELECT * FROM OrderStream o WHERE o.customer_id = this.customer_id;
      EXPORT OrdStream TO print.Orders;
      Amounts := SELECT id, amount AS amount_min, amount AS amount_max FROM OrderStream;
      TwoColumns := SELECT amount_mx FROM Amounts;
      HintOfFlink := SELECT /*+ BROADCST(p) */ o.id
                     FROM OrderStream o JOIN OrderStream p ON o.id = p.id;
      HintInput := SELECT /*+ BROADCAST(pp) */ o.id
                   FROM OrderStream AS o JOIN OrderStream p ON o.id = p.id;
      """;

  /** A script of the project that the main script does not import */
  private static final String HELPERS =
      """
      IMPORT shop.sources;
      BigOrders := SELECT * FROM sources.Orders WHERE amount > 10;
      """;

  @TempDir static Path tempDir;

  private static CheckTestProject project;
  private static CheckResult result;

  @BeforeAll
  static void check() throws Exception {
    project = CheckTestProject.create(tempDir);
    Files.writeString(project.projectRoot().resolve("helpers.sqrl"), HELPERS);
    result =
        new ScriptChecker()
            .check(
                CheckRequest.of(project.projectRoot(), tempDir.resolve("work"))
                    .withOverlays(Map.of(project.mainScript(), SCRIPT))
                    .withKeepCatalog(true));
  }

  @AfterAll
  static void close() {
    result.catalog().ifPresent(catalog -> catalog.close());
  }

  @Test
  void givenMissingDelimiter_whenFix_thenAddsIt() {
    var fixes = fixes("Missing ';'");

    assertThat(fixes).singleElement().satisfies(fix -> assertThat(fix.getIsPreferred()).isTrue());
    assertThat(apply(fixes.get(0)))
        .isEqualTo(SCRIPT.replace("FROM sources.Orders\n", "FROM sources.Orders;\n"));
  }

  @Test
  void givenUnknownTable_whenFix_thenSuggestsTheClosestTable() {
    var fixes = fixes("Object 'OrderStrem' not found");

    assertThat(titles(fixes)).containsExactly("Change to `OrderStream`");
    assertThat(apply(fixes.get(0))).contains("UnknownTable := SELECT * FROM OrderStream;");
  }

  @Test
  void givenUnknownTableOfDatabase_whenFix_thenSuggestsTheClosestTableOfTheDatabase() {
    var fixes = fixes("Object 'Ordrs' not found");

    assertThat(titles(fixes)).containsExactly("Change to `Orders`");
    assertThat(apply(fixes.get(0))).contains("SELECT * FROM sources.Orders;");
  }

  @Test
  void givenUnknownTableInSubquery_whenFix_thenSuggestsTheClosestTable() {
    assertThat(apply(fixes("Object 'OrdrStream' not found").get(0)))
        .contains("(SELECT id FROM OrderStream);");
  }

  @Test
  void givenUnknownTableOfSqrlStatements_whenFix_thenSuggestsTheClosestTable() {
    assertThat(apply(fixes("Could not find parent table for relationship: OrderStrem").get(0)))
        .contains("OrderStream.others := SELECT");
    assertThat(apply(fixes("Could not find table: OrdStream").get(0)))
        .contains("EXPORT OrderStream TO print.Orders;");
  }

  @Test
  void givenSeveralCloseNames_whenFix_thenPrefersTheClosest() {
    var fixes = fixes("Column 'amount_mx' not found");

    assertThat(titles(fixes)).containsExactly("Change to `amount_max`", "Change to `amount_min`");
    assertThat(fixes).extracting(CodeAction::getIsPreferred).containsExactly(true, null);
  }

  @Test
  void givenNameInOtherCase_whenFix_thenSuggestsTheNameOfTheCompiler() {
    var fixes = fixes("Object 'orderstream' not found");

    assertThat(titles(fixes)).containsExactly("Change to `OrderStream`");
    assertThat(fixes.get(0).getIsPreferred()).isTrue();
  }

  @Test
  void givenUnknownColumns_whenFix_thenSuggestsTheClosestColumns() {
    assertThat(apply(fixes("Column 'amout' not found").get(0)))
        .contains("UnknownColumn := SELECT amount FROM OrderStream;");
    assertThat(apply(fixes("Column 'custmer_id' not found").get(0)))
        .contains("UnknownQualifiedColumn := SELECT o.customer_id FROM OrderStream o;");
  }

  @Test
  void givenTableOfAnotherScript_whenFix_thenImportsTheScriptAfterTheOtherImports() {
    var fixes = fixes("Object 'BigOrders' not found");

    // Only the CREATE TABLE tables of a script can be imported by name
    assertThat(titles(fixes)).containsExactly("Add `IMPORT helpers.*;`");
    assertThat(apply(fixes.get(0)))
        .startsWith("IMPORT shop.sources;\nIMPORT helpers.*;\n\nOrderStream := ");
  }

  @Test
  void givenCreatedTableOfPackage_whenFix_thenImportsIt() {
    assertThat(titles(fixes("Object 'Orders' not found")))
        .containsExactly("Add `IMPORT shop.sources.Orders;`");
  }

  @Test
  void givenDatabaseOfAnotherScript_whenFix_thenImportsTheScript() {
    var fixes = fixes("Object 'helpers' not found");

    assertThat(titles(fixes)).containsExactly("Add `IMPORT helpers;`");
  }

  @Test
  void givenMisspelledHint_whenFix_thenSuggestsTheClosestHint() {
    var diagnostic = diagnostic("Unrecognized hint: [primay_key]");
    var fixes = fixes(diagnostic);

    assertThat(diagnostic.getRange()).isEqualTo(new Range(new Position(9, 4), new Position(9, 14)));
    assertThat(titles(fixes)).containsExactly("Change to `primary_key`");
    assertThat(apply(fixes.get(0))).contains("/*+ primary_key(id) */");
  }

  @Test
  void givenMisspelledHintOfFlink_whenFix_thenItIsAtTheHintAndSuggestsTheClosestHint() {
    var diagnostic = diagnostic("BROADCST");
    var line = lineOf("HintOfFlink");
    var start = SCRIPT.lines().toList().get(line).indexOf("BROADCST");

    assertThat(diagnostic.getMessage().getLeft())
        .startsWith("Unknown hint `BROADCST`. Flink's hints are BROADCAST, SHUFFLE_HASH");
    assertThat(diagnostic.getRange())
        .isEqualTo(new Range(new Position(line, start), new Position(line, start + 8)));
    var fixes = fixes(diagnostic);
    assertThat(titles(fixes)).containsExactly("Change to `BROADCAST`");
    assertThat(apply(fixes.get(0))).contains("HintOfFlink := SELECT /*+ BROADCAST(p) */");
  }

  @Test
  void givenHintOfFlinkThatNamesNoInput_whenFix_thenItIsAnErrorAndSuggestsTheInputs() {
    var diagnostic = diagnostic("names `pp`");
    var line = lineOf("HintInput");
    var start = SCRIPT.lines().toList().get(line).indexOf("pp)");

    assertThat(diagnostic.getSeverity()).isEqualTo(DiagnosticSeverity.Error);
    assertThat(diagnostic.getMessage().getLeft())
        .isEqualTo(
            "Hint BROADCAST names `pp`, which is not a table or alias of this query, so Flink"
                + " rejects it. The query has: `o`, `p`.");
    assertThat(diagnostic.getRange())
        .isEqualTo(new Range(new Position(line, start), new Position(line, start + 2)));
    var fixes = fixes(diagnostic);
    assertThat(titles(fixes)).containsExactly("Change to `p`", "Change to `o`");
    assertThat(apply(fixes.get(0))).contains("HintInput := SELECT /*+ BROADCAST(p) */");
    // A hint that names an input is not reported
    assertThat(result.diagnostics()).noneMatch(d -> d.message().contains("names `p`"));
  }

  private static int lineOf(String name) {
    return SCRIPT
        .lines()
        .toList()
        .indexOf(
            SCRIPT.lines().filter(line -> line.startsWith(name + " :=")).findFirst().orElseThrow());
  }

  @Test
  void givenSortColumnsThatAreNotSelected_whenFix_thenSelectsThem() {
    assertThat(apply(fixes("missing: [amount]").get(0)))
        .contains("SortColumn := SELECT id, amount FROM OrderStream ORDER BY amount;");
    var fixes = fixes("customer_id]");
    assertThat(titles(fixes))
        .containsExactly("Add `p.updated_at, o.customer_id` to the SELECT clause");
    assertThat(apply(fixes.get(0)))
        .contains(
            "SortColumns := SELECT o.id, SUM(o.amount) AS total, p.updated_at, o.customer_id\n");
  }

  @Test
  void givenChangedName_whenFix_thenSuggestsNothing() {
    var diagnostic = diagnostic("Object 'OrderStrem' not found");
    var changed = SCRIPT.replace("FROM OrderStrem;", "FROM OrderStrea;");

    assertThat(quickFixes(changed).fix(diagnostic)).isEmpty();
  }

  @Test
  void givenNames_whenClosest_thenFindsTyposSortedByDistance() {
    // Short names allow fewer typos
    assertThat(QuickFixes.closest("amout", List.of("id", "amounts", "amount", "mount")))
        .containsExactly("amount");
    assertThat(QuickFixes.closest("customerid", List.of("id", "customers_id", "customer_id")))
        .containsExactly("customer_id", "customers_id");
    assertThat(QuickFixes.distance("ORDERS", "orders")).isZero();
    assertThat(QuickFixes.distance("primay_key", "primary_key")).isOne();
  }

  private static Diagnostic diagnostic(String message) {
    var diagnostics =
        result.diagnostics().stream().filter(d -> d.message().contains(message)).toList();
    assertThat(diagnostics).as("diagnostics with " + message).hasSize(1);
    return DiagnosticConverter.convert(diagnostics.get(0), SCRIPT);
  }

  private static List<CodeAction> fixes(String message) {
    return fixes(diagnostic(message));
  }

  private static List<CodeAction> fixes(Diagnostic diagnostic) {
    return quickFixes(SCRIPT).fix(diagnostic);
  }

  private static QuickFixes quickFixes(String text) {
    var file = project.mainScript();
    var buildDir = tempDir.resolve("work").resolve("build");
    return new QuickFixes(
        file,
        URI,
        text,
        context ->
            new ScriptCompletion(
                    file, result.catalog(), List.of(result.symbols()), Optional.of(buildDir))
                .complete(context),
        Optional.of(new ImportCandidates(buildDir, file)));
  }

  private static List<String> titles(List<CodeAction> fixes) {
    return fixes.stream().map(CodeAction::getTitle).toList();
  }

  /** The script with the edits of a fix. */
  private static String apply(CodeAction fix) {
    var script = new ScriptText(SCRIPT);
    var edits =
        fix.getEdit().getChanges().get(URI).stream()
            .sorted(
                Comparator.comparingInt((TextEdit e) -> offset(script, e.getRange().getStart()))
                    .reversed())
            .toList();
    var text = new StringBuilder(SCRIPT);
    for (var edit : edits) {
      text.replace(
          offset(script, edit.getRange().getStart()),
          offset(script, edit.getRange().getEnd()),
          edit.getNewText());
    }
    return text.toString();
  }

  private static int offset(ScriptText script, Position position) {
    return script.offset(position.getLine(), position.getCharacter());
  }
}
