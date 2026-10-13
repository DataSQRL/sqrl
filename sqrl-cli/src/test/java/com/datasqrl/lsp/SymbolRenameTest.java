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

import static com.datasqrl.compile.check.CheckTestProject.API_SCRIPT;
import static com.datasqrl.compile.check.CheckTestProject.OPERATIONS;
import static com.datasqrl.compile.check.CheckTestProject.SCHEMA;
import static com.datasqrl.compile.check.CheckTestProject.SHARED_SCRIPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.compile.check.CheckRequest;
import com.datasqrl.compile.check.CheckResult;
import com.datasqrl.compile.check.CheckTestProject;
import com.datasqrl.compile.check.ScriptChecker;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.error.ErrorMessage.Severity;
import com.datasqrl.lsp.SymbolRename.RenameException;
import com.datasqrl.plan.validate.ExecutionGoal;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SymbolRenameTest {

  @TempDir static Path tempDir;

  private static CheckTestProject project;
  private static Map<Path, SymbolIndex> indexes;

  @BeforeAll
  static void check() throws IOException {
    project = CheckTestProject.createWithApi(tempDir);
    var result = new ScriptChecker().check(request(Map.of(), false));
    assertThat(errors(result)).isEmpty();
    indexes = Map.of(project.projectRoot(), result.symbols());
  }

  @Test
  void givenTableOfApi_whenRename_thenRenamesItInScriptsSchemaAndOperations() throws Exception {
    // Orders in `Orders := DISTINCT`
    var rename = rename();
    var plan = rename.plan(rename.prepare(main(), API_SCRIPT, 3, 2), "Purchases");
    var renamed = rename.apply(plan);

    assertThat(renamed.get(project.mainScript()))
        .isEqualTo(
            API_SCRIPT
                .replace("Orders := DISTINCT", "Purchases := DISTINCT")
                .replace("FROM Orders GROUP", "FROM Purchases GROUP")
                .replace("Orders.totals :=", "Purchases.totals :="));
    assertThat(renamed.get(project.schema()))
        .isEqualTo(
            SCHEMA
                .replace("type Orders {", "type Purchases {")
                .replace("  Orders(limit", "  Purchases(limit")
                .replace("[Orders!]", "[Purchases!]"));
    assertThat(renamed.get(project.operations()))
        .isEqualTo(OPERATIONS.replace("orders: Orders(", "orders: Purchases("));
    assertThat(renamed).doesNotContainKey(project.sharedScript());
    assertThat(plan.changesApi()).isTrue();
    assertThat(plan.projects()).containsExactly(project.projectRoot());
    // The renamed project still compiles, including its API
    assertThat(errors(new ScriptChecker().check(request(renamed, true)))).isEmpty();
  }

  @Test
  void givenTableOfIncludedScript_whenRename_thenRenamesTheNameButNotItsDatabase()
      throws Exception {
    // Orders in `sources.Orders`
    var rename = rename();
    var plan = rename.plan(rename.prepare(main(), API_SCRIPT, 2, 40), "Purchases");
    var renamed = rename.apply(plan);

    assertThat(renamed.get(project.sharedScript()))
        .isEqualTo(SHARED_SCRIPT.replace("CREATE TABLE Orders", "CREATE TABLE Purchases"));
    assertThat(renamed.get(project.mainScript()))
        .isEqualTo(API_SCRIPT.replace("sources.Orders", "sources.Purchases"));
    // Only tables of the main script are part of the API
    assertThat(plan.changesApi()).isFalse();
    assertThat(errors(new ScriptChecker().check(request(renamed, false)))).isEmpty();
  }

  @Test
  void givenRelationship_whenRename_thenRenamesItsFieldInTheApi() throws Exception {
    // totals in `Orders.totals :=`
    var rename = rename();
    var plan = rename.plan(rename.prepare(main(), API_SCRIPT, 5, 9), "customerTotals");
    var renamed = rename.apply(plan);

    assertThat(renamed.get(project.mainScript()))
        .isEqualTo(API_SCRIPT.replace("Orders.totals", "Orders.customerTotals"));
    assertThat(renamed.get(project.schema()))
        .isEqualTo(SCHEMA.replace("  totals(limit", "  customerTotals(limit"));
    assertThat(renamed.get(project.operations()))
        .isEqualTo(OPERATIONS.replace("totals {", "customerTotals {"));
    assertThat(errors(new ScriptChecker().check(request(renamed, true)))).isEmpty();
  }

  @Test
  void givenPositions_whenPrepare_thenFindsTheNameAtThePosition() throws Exception {
    var rename = rename();

    // CustomerTotals in `FROM CustomerTotals t` of the relationship
    var target = rename.prepare(main(), API_SCRIPT, 5, 35);
    assertThat(target.symbol().name()).isEqualTo("CustomerTotals");
    assertThat(target.range().startCharacter()).isEqualTo(31);
    assertThat(target.range().endCharacter()).isEqualTo(45);
    // Orders in `Orders.totals :=` is the table, not the relationship
    assertThat(rename.prepare(main(), API_SCRIPT, 5, 3).symbol().name()).isEqualTo("Orders");
  }

  @Test
  void givenNameThatIsTaken_whenRename_thenRefuses() throws Exception {
    var rename = rename();
    var orders = rename.prepare(main(), API_SCRIPT, 3, 2);
    var totals = rename.prepare(main(), API_SCRIPT, 5, 9);
    var customerTotals = rename.prepare(main(), API_SCRIPT, 4, 2);
    assertThat(totals.symbol().identifier().getObjectName()).isEqualTo("Orders.totals");

    assertThatThrownBy(() -> rename.plan(orders, "customertotals"))
        .isInstanceOf(RenameException.class)
        .hasMessage("`CustomerTotals` already exists in `main.sqrl` line 5.");
    assertThatThrownBy(() -> rename.plan(totals, "amount"))
        .hasMessage("`Orders` already has a column named `amount`.");
    assertThatThrownBy(() -> rename.plan(customerTotals, "Long"))
        .hasMessage("The GraphQL schema already has a type named `Long`.");
  }

  @Test
  void givenInvalidName_whenRename_thenRefuses() throws Exception {
    var rename = rename();
    var orders = rename.prepare(main(), API_SCRIPT, 3, 2);

    assertThatThrownBy(() -> rename.plan(orders, "1st"))
        .hasMessageStartingWith("`1st` is not a valid name");
    assertThatThrownBy(() -> rename.plan(orders, "Select"))
        .hasMessage("`Select` is a reserved word in SQL.");
    assertThat(rename.plan(orders, "Orders").edits()).isEmpty();
  }

  @Test
  void givenColumnOrUncheckedChange_whenPrepare_thenRefuses() {
    var rename = rename();

    // customer_id in `GROUP BY customer_id`
    assertThatThrownBy(() -> rename.prepare(main(), API_SCRIPT, 4, 85))
        .hasMessage("Columns cannot be renamed yet.");
    assertThatThrownBy(() -> rename.prepare(main(), API_SCRIPT + "\n", 3, 2))
        .hasMessage(
            "`main.sqrl` changed since it was last checked, try again when the check is done.");
    // The keyword DISTINCT
    assertThatThrownBy(() -> rename.prepare(main(), API_SCRIPT, 3, 12))
        .hasMessage("Only tables, functions and relationships can be renamed.");
  }

  @Test
  void givenTableOfApi_whenFindInApi_thenFindsItsFieldsAndType() {
    var rename = rename();
    var orders =
        indexes.get(project.projectRoot()).symbolsNamed("Orders").stream()
            .filter(s -> s.identifier().getDatabaseName().equals("default_database"))
            .findFirst()
            .orElseThrow();

    var found = rename.findInApi(orders);

    // The type, its query field and the result type of that field, and the field in the operation
    assertThat(found).singleElement().satisfies(f -> assertThat(f.ranges()).hasSize(4));
  }

  private static SymbolRename rename() {
    return new SymbolRename(indexes, SymbolRenameTest::read);
  }

  private static Path main() {
    return project.mainScript();
  }

  private static Optional<String> read(Path file) {
    try {
      return Optional.of(Files.readString(file));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private static CheckRequest request(Map<Path, String> overlays, boolean compile) {
    return new CheckRequest(
        project.projectRoot(),
        List.of(project.projectRoot().resolve("package.json")),
        tempDir.resolve(compile ? "compile" : "work"),
        overlays,
        compile,
        compile ? ExecutionGoal.COMPILE : ExecutionGoal.TEST,
        false);
  }

  private static List<CheckResult.Diagnostic> errors(CheckResult result) {
    return result.diagnostics().stream().filter(d -> d.severity() == Severity.FATAL).toList();
  }
}
