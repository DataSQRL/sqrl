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

import static com.datasqrl.compile.check.CheckTestProject.MAIN_SCRIPT;
import static com.datasqrl.compile.check.CheckTestProject.SHARED_SCRIPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.compile.check.CheckResult.Diagnostic;
import com.datasqrl.compile.check.SymbolIndex.ImportLink;
import com.datasqrl.compile.check.SymbolIndex.Reference;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.error.ErrorLocation.FileRange;
import com.datasqrl.error.ErrorMessage.Severity;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ScriptCheckerTest {

  @TempDir Path tempDir;

  private CheckTestProject project;
  private final ScriptChecker checker = new ScriptChecker();

  @BeforeEach
  void setUp() throws Exception {
    project = CheckTestProject.create(tempDir);
  }

  @Test
  void givenValidProject_whenCheck_thenReportsNoErrorsAndPlannedTables() {
    var result = checker.check(request());

    assertThat(errors(result)).isEmpty();
    assertThat(result.symbols().symbolsNamed("CustomerTotals"))
        .singleElement()
        .satisfies(
            table ->
                assertThat(table.columns())
                    .extracting(SymbolIndex.Column::name)
                    .containsExactly("customer_id", "total"));
    assertThat(result.sourceFiles()).contains(project.mainScript(), project.sharedScript());
    assertThat(project.projectRoot().resolve("build")).doesNotExist();
  }

  @Test
  void givenOtherScriptInTheProjectDirectory_whenCheck_thenItIsNoSourceFileOfTheProject()
      throws IOException {
    var other =
        Files.writeString(
            project.mainScript().resolveSibling("other.sqrl"), "Other := SELECT 1 AS one;\n");

    var result = checker.check(request());

    assertThat(result.sourceFiles())
        .contains(project.mainScript(), project.sharedScript())
        .doesNotContain(other);
  }

  @Test
  void givenValidProject_whenCheck_thenIndexesDefinitionsReferencesAndImports() {
    var symbols = checker.check(request()).symbols();
    var main = project.mainScript();

    assertThat(symbols.symbol(ObjectIdentifier.of("default_catalog", "sources", "Orders")))
        .hasValueSatisfying(
            table ->
                assertThat(table.location())
                    .isEqualTo(new SourceRange(project.sharedScript(), 0, 13, 0, 19)));
    assertThat(symbols.symbol(ObjectIdentifier.of("default_catalog", "default_database", "Orders")))
        .hasValueSatisfying(
            table -> {
              assertThat(table.location()).isEqualTo(new SourceRange(main, 3, 0, 3, 6));
              assertThat(table.tableType()).isEqualTo("VERSIONED_STATE");
              assertThat(table.primaryKey()).containsExactly("id");
            });
    assertThat(symbols.references())
        .extracting(
            r -> r.location().startLine(),
            r -> r.location().startCharacter(),
            r -> r.location().endCharacter(),
            r -> r.table().getDatabaseName() + "." + r.table().getObjectName(),
            Reference::column)
        .containsExactlyInAnyOrder(
            tuple(2, 29, 43, "sources.Orders", List.of()),
            tuple(3, 19, 30, "default_database.OrderStream", List.of()),
            tuple(3, 34, 36, "default_database.OrderStream", List.of("id")),
            tuple(3, 46, 56, "default_database.OrderStream", List.of("updated_at")),
            tuple(4, 25, 36, "default_database.Orders", List.of("customer_id")),
            tuple(4, 42, 48, "default_database.Orders", List.of("amount")),
            tuple(4, 64, 70, "default_database.Orders", List.of()),
            tuple(4, 80, 91, "default_database.Orders", List.of("customer_id")));
    assertThat(symbols.imports())
        .containsExactly(
            new ImportLink(
                new SourceRange(main, 0, 7, 0, 19), SourceRange.at(project.sharedScript(), 0, 0)));
    assertThat(symbols.isCurrent(main, MAIN_SCRIPT)).isTrue();
  }

  @Test
  void givenTableNamesOutsideOfQueriesAndAsQualifiers_whenCheck_thenIndexesThemAsReferences() {
    var script =
        """
        IMPORT shop.sources;
        OrderStream := SELECT * FROM sources.Orders;
        OrderStream.big := amount > 10;
        Orders := DISTINCT OrderStream ON id ORDER BY updated_at DESC;
        Totals := SELECT customer_id, SUM(amount) AS total FROM Orders GROUP BY customer_id;
        Orders.totals := SELECT * FROM Totals t WHERE t.customer_id = this.customer_id;
        EXPORT Totals TO print.Totals;
        Big := SELECT Orders.id FROM Orders WHERE Orders.big;
        """;

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    assertThat(errors(result)).isEmpty();
    var lines = script.lines().toList();
    assertThat(tableReferences(result.symbols(), "OrderStream"))
        .containsExactlyInAnyOrder(at(lines, 2, "OrderStream.big"), at(lines, 3, "OrderStream ON"));
    assertThat(tableReferences(result.symbols(), "Orders"))
        .containsExactlyInAnyOrder(
            at(lines, 4, "Orders GROUP"),
            at(lines, 5, "Orders.totals"),
            at(lines, 7, "Orders.id"),
            at(lines, 7, "Orders WHERE"),
            at(lines, 7, "Orders.big"));
    assertThat(tableReferences(result.symbols(), "Totals"))
        .containsExactlyInAnyOrder(at(lines, 5, "Totals t"), at(lines, 6, "Totals TO"));
  }

  @Test
  void givenAliasInsertAndFunctionCall_whenCheck_thenIndexesThem() {
    var script =
        """
        IMPORT shop.sources.Orders AS SourceOrders;
        OrderStream := SELECT * FROM SourceOrders;
        CREATE TABLE Sink (id BIGINT) WITH ('connector' = 'print');
        INSERT INTO Sink SELECT id FROM OrderStream;
        OrdersOf(customer BIGINT) := SELECT * FROM OrderStream WHERE customer_id = :customer;
        /*+test */
        OrdersOfTest := SELECT * FROM TABLE(OrdersOf(10));
        """;

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    assertThat(errors(result)).isEmpty();
    var lines = script.lines().toList();
    // The alias defines the imported table, and the path leads to the imported definition
    assertThat(result.symbols().symbolsNamed("SourceOrders"))
        .singleElement()
        .satisfies(
            table ->
                assertThat(table.location())
                    .isEqualTo(new SourceRange(project.mainScript(), 0, 30, 0, 42)));
    assertThat(result.symbols().imports())
        .containsExactly(
            new ImportLink(
                new SourceRange(project.mainScript(), 0, 7, 0, 26),
                new SourceRange(project.sharedScript(), 0, 13, 0, 19)));
    assertThat(tableReferences(result.symbols(), "SourceOrders"))
        .containsExactly(at(lines, 1, "SourceOrders"));
    assertThat(tableReferences(result.symbols(), "Sink")).containsExactly(at(lines, 3, "Sink"));
    assertThat(tableReferences(result.symbols(), "OrdersOf"))
        .containsExactly(at(lines, 6, "OrdersOf("));
  }

  @Test
  void givenAddedColumn_whenCheck_thenIndexesItWithoutTheQueryOfItsTable() {
    var script =
        MAIN_SCRIPT.replace(
            "Orders := DISTINCT", "OrderStream.big := amount > 10;\nOrders := DISTINCT");

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    assertThat(errors(result)).isEmpty();
    assertThat(result.symbols().symbolsNamed("OrderStream"))
        .singleElement()
        .satisfies(
            table -> {
              assertThat(table.columns()).extracting(SymbolIndex.Column::name).contains("big");
              assertThat(table.columnLocations())
                  .containsExactly(
                      Map.entry("big", new SourceRange(project.mainScript(), 3, 0, 3, 15)));
            });
    // The query of OrderStream is planned again for the added column, but only indexed once
    assertThat(result.symbols().references())
        .filteredOn(r -> r.table().getDatabaseName().equals("sources"))
        .singleElement()
        .satisfies(r -> assertThat(r.location().startLine()).isEqualTo(2));
  }

  @Test
  void givenKeepCatalog_whenCheck_thenCompletesQueriesUntilClosed() {
    var result = checker.check(request().withKeepCatalog(true));
    assertThat(checker.check(request()).catalog()).isEmpty();

    var catalog = result.catalog().orElseThrow();
    var completions = catalog.complete("SELECT * FROM ", 14, "default_database");
    catalog.close();

    assertThat(completions.suggestions())
        .extracting(PlannedCatalog.Suggestion::name)
        .contains("Orders", "CustomerTotals");
    assertThat(catalog.complete("SELECT * FROM ", 14, "default_database").suggestions()).isEmpty();
  }

  @Test
  void givenTwoIndependentErrors_whenCheck_thenReportsBothButNoKnockOnErrors() {
    var script =
        MAIN_SCRIPT.replace("SELECT * FROM sources.Orders", "SELECT *, bad1 FROM sources.Orders")
            + "Other := SELECT bad2 FROM sources.Orders;\n";

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    // Orders and CustomerTotals use the failed OrderStream, so they are not reported
    assertThat(errors(result))
        .extracting(Diagnostic::message, Diagnostic::range)
        .containsExactly(
            tuple("Column 'bad1' not found in any table", range(3, 26)),
            tuple("Column 'bad2' not found in any table", range(6, 17)));
  }

  @Test
  void givenErrorInDistinct_whenCheck_thenReportsItInTheOnClauseWithoutKnockOnErrors() {
    var script = MAIN_SCRIPT.replace("ON id ORDER", "ON idx ORDER");

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    // CustomerTotals uses the failed Orders table, so it is not reported
    assertThat(errors(result))
        .extracting(Diagnostic::message, Diagnostic::range)
        .containsExactly(tuple("Column 'idx' not found in any table", range(4, 35)));
  }

  @Test
  void givenMissingSemicolon_whenCheck_thenReportsItAndPlansBothStatements() {
    var script = MAIN_SCRIPT.replace("FROM sources.Orders;", "FROM sources.Orders");

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    assertThat(errors(result))
        .extracting(Diagnostic::message, Diagnostic::range)
        .containsExactly(tuple("Missing ';' at the end of the statement", range(3, 44)));
    assertThat(result.symbols().symbols())
        .extracting(SymbolIndex.Symbol::name)
        .contains("CustomerTotals");
  }

  @Test
  void givenErrorInUnsavedTemplatedImport_whenCheck_thenReportsItInTheSourceFile()
      throws Exception {
    var sharedScript = SHARED_SCRIPT.replace("PRIMARY KEY (id,", "PRIMARY KEY (idx,");

    var result =
        checker.check(request().withOverlays(Map.of(project.sharedScript(), sharedScript)));

    assertThat(errors(result))
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.message())
                  .isEqualTo("Primary key column 'idx' is not defined in the schema");
              assertThat(error.file()).contains(project.sharedScript());
              assertThat(error.range()).isEqualTo(range(2, 16));
            });
    assertThat(Files.readString(project.sharedScript())).isEqualTo(SHARED_SCRIPT);
  }

  @Test
  void givenTemplateBeforeNamesOnALine_whenCheck_thenReportsAndIndexesThemInTheTemplate()
      throws Exception {
    var packageFile = project.projectRoot().resolve("package.json");
    Files.writeString(
        packageFile,
        Files.readString(packageFile)
            .replace(
                "\"main\": \"main.sqrl\",",
                "\"main\": \"main.sqrl\", \"config\": {\"db\": \"sources\"},"));
    var script =
        MAIN_SCRIPT.replace(
                "OrderStream := SELECT * FROM sources.Orders;",
                "OrderStream := SELECT * FROM {{db}}.Orders WHERE amount > 0;")
            + "Big := SELECT * FROM {{db}}.Orders WHERE amout > 0;\n";

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    var lines = script.lines().toList();
    assertThat(errors(result))
        .singleElement()
        .satisfies(
            error ->
                assertThat(error.range()).isEqualTo(range(6, lines.get(5).indexOf("amout") + 1)));
    // The name of the table after the template, e.g. `{{db}}.Orders`
    assertThat(result.symbols().references())
        .filteredOn(r -> r.location().startLine() == 2 && r.column().isEmpty())
        .extracting(r -> r.location().endCharacter())
        .contains(lines.get(2).indexOf(".Orders") + ".Orders".length());
  }

  @Test
  void givenTemplateSections_whenCheck_thenReportsAndIndexesTheLinesAfterThemInTheTemplate()
      throws Exception {
    var packageFile = project.projectRoot().resolve("package.json");
    Files.writeString(
        packageFile,
        Files.readString(packageFile)
            .replace(
                "\"main\": \"main.sqrl\",",
                "\"main\": \"main.sqrl\", \"config\": {\"test\": true},"));
    var script =
        MAIN_SCRIPT
            + """
            {{#test}}
            TestOrders := SELECT * FROM Orders;
            {{/test}}
            {{^test}}
            ProdOrders := SELECT * FROM Orders;
            {{/test}}
            Big := SELECT * FROM Orders WHERE amout > 0;
            """;

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    // The lines of the section tags are not rendered, so the lines below them moved up
    var lines = script.lines().toList();
    var big = lines.indexOf("Big := SELECT * FROM Orders WHERE amout > 0;");
    assertThat(errors(result))
        .singleElement()
        .satisfies(
            error ->
                assertThat(error.range())
                    .isEqualTo(range(big + 1, lines.get(big).indexOf("amout") + 1)));
    assertThat(result.symbols().symbolsNamed("TestOrders"))
        .singleElement()
        .satisfies(
            symbol ->
                assertThat(symbol.location().startLine())
                    .isEqualTo(lines.indexOf("TestOrders := SELECT * FROM Orders;")));
  }

  @Test
  void givenImportFromDirectoryWithHyphen_whenCheck_thenLinksTheWholePath() throws IOException {
    var tables =
        Files.createDirectories(project.projectRoot().resolve("my-tables")).resolve("defs.sqrl");
    Files.writeString(tables, "Numbers := SELECT 1 AS n;\n");
    var script = "IMPORT my-tables.defs.*;\nX := SELECT n FROM Numbers;\n";

    var result = checker.check(request().withOverlays(Map.of(project.mainScript(), script)));

    assertThat(errors(result)).isEmpty();
    assertThat(result.symbols().imports())
        .containsExactly(
            new ImportLink(
                new SourceRange(project.mainScript(), 0, 7, 0, "IMPORT my-tables.defs.*".length()),
                SourceRange.at(tables, 0, 0)));
  }

  @Test
  void givenFullCompile_whenCheck_thenReportsNoErrors() {
    var result = checker.check(request().withFullCompile(true));

    assertThat(errors(result)).isEmpty();
  }

  @Test
  void givenNewerCheck_whenCheck_thenStopsBeforePlanning() {
    assertThatThrownBy(() -> checker.check(request(), () -> true))
        .isInstanceOf(CancellationException.class);
  }

  @Test
  void givenNewerCheckWhileAnImportedScriptIsPlanned_whenCheck_thenStopsThere() {
    var statements = new AtomicInteger();

    // The first statement of the main script imports the shared script, whose first statement is
    // the second one
    assertThatThrownBy(() -> checker.check(request(), () -> statements.incrementAndGet() > 1))
        .isInstanceOf(CancellationException.class);
    assertThat(statements).hasValue(2);
  }

  /** The ranges of the references to a table of the main script, as {@code line:start-end}. */
  private static List<String> tableReferences(SymbolIndex symbols, String table) {
    return symbols.references().stream()
        .filter(r -> !r.isColumn() && r.table().getDatabaseName().equals("default_database"))
        .filter(r -> r.table().getObjectName().equals(table))
        .map(Reference::location)
        .map(r -> r.startLine() + ":" + r.startCharacter() + "-" + r.endCharacter())
        .toList();
  }

  /**
   * The range of a table name in a line, found as the first word of the given text, e.g. {@code
   * Orders} for {@code "Orders W"}.
   */
  private static String at(List<String> lines, int line, String text) {
    var name = text.split("[ .(]")[0];
    var start = lines.get(line).indexOf(text);
    assertThat(start).as("%s in line %d", text, line).isNotNegative();
    return line + ":" + start + "-" + (start + name.length());
  }

  private CheckRequest request() {
    return CheckRequest.of(project.projectRoot(), tempDir.resolve("work"));
  }

  private static List<Diagnostic> errors(CheckResult result) {
    return result.diagnostics().stream().filter(d -> d.severity() == Severity.FATAL).toList();
  }

  private static Optional<FileRange> range(int line, int column) {
    return Optional.of(new FileRange(line, column, line, column));
  }
}
