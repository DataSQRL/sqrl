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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.planner.hint.HintsAndDoc;
import com.datasqrl.planner.parser.SqrlStatementParser;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests the knock-on errors with the errors of the Flink validator and catalog. */
class FailedObjectsTest {

  @TempDir Path workspace;

  private final SqrlStatementParser parser = new SqrlStatementParser();

  /** The failures as seen from the main script */
  private final FailedObjects failures = new FailedObjects();

  /** The failures as seen from a script that is imported as {@code sources} */
  private final FailedObjects sources = failures.forImportedScript("sources");

  private Sqrl2FlinkSQLTranslator translator;

  @BeforeEach
  void setUp() {
    var paths = new WorkspacePaths(workspace, workspace, workspace, workspace);
    var flink = mock(FlinkStreamEngine.class);
    when(flink.getExecutionMode()).thenReturn(RuntimeExecutionMode.STREAMING);
    when(flink.getBaseConfiguration()).thenReturn(new Configuration());
    when(flink.getStreamingSpecificConfig()).thenReturn(new Configuration());
    var config = mock(CompilerConfig.class);
    when(config.predicatePushdownRules()).thenReturn(PredicatePushdownRules.DEFAULT);
    translator = new Sqrl2FlinkSQLTranslator(paths, flink, config, ErrorCollector.root());
    translator.createTable(
        "CREATE TABLE Orders (id INT, customer STRING, amount DOUBLE)"
            + " WITH ('connector'='datagen')",
        Optional.empty(),
        mock(SchemaLoader.class),
        HintsAndDoc.EMPTY,
        ErrorCollector.root());
  }

  @AfterEach
  void tearDown() {
    translator.close();
  }

  @Test
  void givenFailedTable_whenQueryUsesIt_thenItsErrorIsKnockOn() {
    record(failures, "Customer := SELECT * FROM CustomerUpdates;");

    assertThat(isKnockOn("SELECT * FROM Orders o JOIN Customer c ON o.customer = c.name")).isTrue();
    assertThat(isKnockOn("SELECT * FROM Customers")).isFalse();
  }

  @Test
  void givenFailedTable_whenQueryHasColumnOfItsName_thenItsOwnErrorIsReported() {
    record(failures, "Customer := SELECT * FROM CustomerUpdates;");

    assertThat(isKnockOn("SELECT customer, missing FROM Orders GROUP BY customer")).isFalse();
  }

  @Test
  void givenFailedTableOfImportedScript_whenUsedByItOrByItsNamespace_thenItsErrorIsKnockOn() {
    translator.executeSQL("CREATE DATABASE sources");
    record(sources, "Totals := SELECT missing FROM Orders;");

    // In the imported script by its own name, and in the main script by its namespace
    assertThat(isKnockOn(sources, "SELECT * FROM Totals")).isTrue();
    assertThat(isKnockOn("SELECT * FROM sources.Totals")).isTrue();
    assertThat(isKnockOn("SELECT * FROM Totals")).isFalse();
  }

  @Test
  void givenFailedTableOfMainScript_whenImportedScriptUsesItsName_thenItsErrorIsReported() {
    record(failures, "Totals := SELECT missing FROM Orders;");

    assertThat(isKnockOn(sources, "SELECT * FROM Totals")).isFalse();
  }

  @Test
  void givenFailedStarImport_whenTableOrFunctionIsNotFound_thenItsErrorIsKnockOn() {
    record(failures, "IMPORT ecomerce.*;");

    assertThat(isKnockOn("SELECT * FROM Customer")).isTrue();
    assertThat(isKnockOn("SELECT myudf(id) FROM Orders")).isTrue();
    // Other errors, and the tables of other namespaces, are still reported
    assertThat(isKnockOn("SELECT missing FROM Orders")).isFalse();
    assertThat(isKnockOn(sources, "SELECT * FROM Customer")).isFalse();
  }

  @Test
  void givenFailedTableOfStarImport_whenUsed_thenOnlyItsErrorIsKnockOn() {
    // The other tables of the package are still imported
    failures.recordImported("Customer");

    assertThat(isKnockOn("SELECT * FROM Customer")).isTrue();
    assertThat(isKnockOn("SELECT * FROM Product")).isFalse();
  }

  @Test
  void givenStatementsThatFailedToParse_whenTheirObjectsAreUsed_thenTheirErrorsAreKnockOn() {
    recordUnparsed("/*+ primary_key(id) junk */\nCustomer := DISTINCT CustomerUpdates ON id;");
    recordUnparsed("IMPORT ecommerce.Product AS Item extra;");
    recordUnparsed("Orders.total := ;");

    assertThat(isKnockOn("SELECT * FROM Customer")).isTrue();
    assertThat(isKnockOn("SELECT * FROM Item")).isTrue();
    assertThat(isKnockOn("SELECT id FROM Orders WHERE total > 10")).isTrue();
    assertThat(isKnockOn("SELECT * FROM Product")).isFalse();
  }

  @Test
  void givenFailedTableOfDatabase_whenUsedByQualifiedName_thenItsErrorIsKnockOn() {
    translator.executeSQL("CREATE DATABASE mydb");
    record(failures, "CREATE TABLE `mydb`.`Totals` (id INT) WITH ('connector' = 'x');");

    assertThat(isKnockOn("SELECT * FROM mydb.Totals")).isTrue();
    assertThat(isKnockOn("SELECT * FROM mydb.Others")).isFalse();
  }

  @Test
  void givenFailedColumnAndFunction_whenUsed_thenTheirErrorsAreKnockOn() {
    record(failures, "Orders.total := amount * missing;");
    record(failures, "CREATE FUNCTION myudf AS 'com.example.Missing';");

    assertThat(isKnockOn("SELECT id FROM Orders WHERE total > 10")).isTrue();
    assertThat(isKnockOn("SELECT o.total FROM Orders o")).isTrue();
    assertThat(isKnockOn("SELECT myudf(id) FROM Orders")).isTrue();
    assertThat(isKnockOn("SELECT otherudf(id) FROM Orders")).isFalse();
  }

  @Test
  void givenFailedColumn_whenUsedThroughTableThatSelectsAllOfItsColumns_thenItsErrorIsKnockOn() {
    record(failures, "Orders.total := amount * missing;");
    translator.addView(
        "CREATE VIEW OrderView AS SELECT * FROM Orders",
        HintsAndDoc.EMPTY,
        false,
        ErrorCollector.root());

    assertThat(isKnockOn("SELECT id, total FROM OrderView")).isTrue();
  }

  @Test
  void givenFailedTable_whenColumnIsAddedToItOrItIsExportedOrInsertedInto_thenItIsDependent() {
    record(failures, "Customer := SELECT * FROM CustomerUpdates;");
    record(failures, "CREATE TABLE Sink (id INT) WITH ('connector' = 'x');");

    assertThat(isDependent("Customer.total := amount * 2;")).isTrue();
    assertThat(isDependent("EXPORT Customer TO print.Customer;")).isTrue();
    assertThat(isDependent("EXPORT Orders TO Sink;")).isTrue();
    assertThat(isDependent("-- the sink\nINSERT INTO `Sink` SELECT id FROM Orders;")).isTrue();
    // The planner looks up the tables of a passthrough query before it validates the query
    assertThat(isDependent("Passthrough RETURNS (id INT) := SELECT id FROM Customer;")).isTrue();
    assertThat(isDependent("Orders.related RETURNS (id INT) := SELECT id FROM Customer;")).isTrue();
    assertThat(isDependent("Orders.total := amount * 2;")).isFalse();
    assertThat(isDependent("Passthrough RETURNS (id INT) := SELECT id FROM Orders;")).isFalse();
    assertThat(isDependent("EXPORT Orders TO print.Orders;")).isFalse();
  }

  @Test
  void givenFailedStarImport_whenStatementUsesTable_thenItIsDependentOnlyIfTableIsNotDefined() {
    record(failures, "IMPORT ecomerce.*;");

    // The tables of the package are unknown, so a table that is not defined may be one of them
    assertThat(isDependent("EXPORT Customer TO print.Customer;")).isTrue();
    assertThat(isDependent("INSERT INTO Sink SELECT id FROM Orders;")).isTrue();
    // A table that is defined is not one of them, so the errors of its statements are reported
    assertThat(isDependent("Orders.total := amount * 2;", "Orders")).isFalse();
    assertThat(isDependent("EXPORT Orders TO print.Orders;", "Orders")).isFalse();
    assertThat(isDependent("INSERT INTO Sink SELECT id FROM Orders;", "Sink")).isFalse();
  }

  @Test
  void givenFailedCatalog_whenUsed_thenItsErrorIsKnockOnAndTheContextFails() {
    record(failures, "CREATE CATALOG iceberg WITH ('type' = 'iceberg');");

    var useCatalog = "USE CATALOG iceberg;";
    var error = catchThrowable(() -> translator.executeSQL("USE CATALOG iceberg"));
    assertThat(failures.isKnockOnError(error)).isTrue();
    assertThat(failures.isContextFailed()).isFalse();

    record(failures, useCatalog);

    // The statements after it would run in another catalog than intended
    assertThat(failures.isContextFailed()).isTrue();
  }

  @Test
  void givenFailedUseInImportedScript_whenBackInImportingScript_thenItsContextIsNotFailed() {
    // An imported script has a context of its own
    record(sources, "/* the database */ USE my_database;");

    assertThat(sources.isContextFailed()).isTrue();
    assertThat(failures.isContextFailed()).isFalse();
  }

  @Test
  void givenObjectThatAlreadyFailed_whenItFailsAgain_thenTheFailureIsCounted() {
    failures.recordImported("Customer");
    var mark = failures.mark();

    // E.g. a star import of a package whose Customer table was imported before and failed
    sources.recordImported("Customer");

    assertThat(failures.recordedSince(mark)).isTrue();
  }

  private StatementObjects objects(String statement) {
    return StatementObjects.of(parser.parseStatement(statement), translator::parseSQL);
  }

  private void record(FailedObjects scriptFailures, String statement) {
    scriptFailures.record(objects(statement));
  }

  private void recordUnparsed(String statement) {
    failures.record(StatementObjects.ofUnparsed(statement));
  }

  private boolean isKnockOn(String query) {
    return isKnockOn(failures, query);
  }

  /** Whether the error of planning a definition with the query is a knock-on error */
  private boolean isKnockOn(FailedObjects scriptFailures, String query) {
    var error =
        catchThrowable(
            () ->
                translator.addView(
                    "CREATE VIEW Q AS " + query, HintsAndDoc.EMPTY, false, ErrorCollector.root()));
    assertThat(error).as(query).isNotNull();
    return scriptFailures.isKnockOnError(error);
  }

  private boolean isDependent(String statement, String... definedTables) {
    var defined = Set.of(definedTables);
    return failures.isDependent(
        objects(statement), path -> defined.contains(String.join(".", path)));
  }
}
