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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests the knock-on errors with the errors of the Flink validator and catalog. */
class FailedTablesTest {

  @TempDir Path workspace;

  private final SqrlStatementParser parser = new SqrlStatementParser();
  private final FailedTables failedTables = new FailedTables();
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
    translator = new Sqrl2FlinkSQLTranslator(paths, flink, config);
    translator.createTable(
        "CREATE TABLE Orders (id INT, customer STRING, amount DOUBLE)"
            + " WITH ('connector'='datagen')",
        Optional.empty(),
        mock(SchemaLoader.class),
        HintsAndDoc.EMPTY);
  }

  @AfterEach
  void tearDown() {
    translator.close();
  }

  @Test
  void givenFailedTable_whenQueryUsesIt_thenItsErrorIsKnockOn() {
    recordFailure("Customer := SELECT * FROM CustomerUpdates;", null);

    assertThat(isKnockOn("SELECT * FROM Orders o JOIN Customer c ON o.customer = c.name", null))
        .isTrue();
    assertThat(isKnockOn("SELECT * FROM Customers", null)).isFalse();
  }

  @Test
  void givenFailedTable_whenQueryHasColumnOfItsName_thenItsOwnErrorIsReported() {
    recordFailure("Customer := SELECT * FROM CustomerUpdates;", null);

    assertThat(isKnockOn("SELECT customer, missing FROM Orders GROUP BY customer", null)).isFalse();
  }

  @Test
  void givenFailedTableOfImportedScript_whenUsedByItOrByItsNamespace_thenItsErrorIsKnockOn() {
    translator.executeSQL("CREATE DATABASE sources");
    recordFailure("Totals := SELECT missing FROM Orders;", "sources");

    // In the imported script by its own name, and in the main script by its namespace
    assertThat(isKnockOn("SELECT * FROM Totals", "sources")).isTrue();
    assertThat(isKnockOn("SELECT * FROM sources.Totals", null)).isTrue();
    assertThat(isKnockOn("SELECT * FROM Totals", null)).isFalse();
  }

  @Test
  void givenFailedTableOfMainScript_whenImportedScriptUsesItsName_thenItsErrorIsReported() {
    recordFailure("Totals := SELECT missing FROM Orders;", null);

    assertThat(isKnockOn("SELECT * FROM Totals", "sources")).isFalse();
  }

  @Test
  void givenFailedStarImport_whenTableOrFunctionIsNotFound_thenItsErrorIsKnockOn() {
    recordFailure("IMPORT ecomerce.*;", null);

    assertThat(isKnockOn("SELECT * FROM Customer", null)).isTrue();
    assertThat(isKnockOn("SELECT myudf(id) FROM Orders", null)).isTrue();
    // Other errors, and the tables of other namespaces, are still reported
    assertThat(isKnockOn("SELECT missing FROM Orders", null)).isFalse();
    assertThat(isKnockOn("SELECT * FROM Customer", "sources")).isFalse();
  }

  @Test
  void givenFailedTableOfStarImport_whenUsed_thenOnlyItsErrorIsKnockOn() {
    // The other tables of the package are still imported
    failedTables.recordImported("Customer", null);
    failedTables.recordFailure(
        parser.parseStatement("IMPORT ecommerce.*;"), "IMPORT ecommerce.*;", true, null);

    assertThat(isKnockOn("SELECT * FROM Customer", null)).isTrue();
    assertThat(isKnockOn("SELECT * FROM Product", null)).isFalse();
  }

  @Test
  void givenStatementsThatFailedToParse_whenTheirObjectsAreUsed_thenTheirErrorsAreKnockOn() {
    failedTables.recordUnparsed(
        "/*+ primary_key(id) junk */\nCustomer := DISTINCT CustomerUpdates ON id;", null);
    failedTables.recordUnparsed("IMPORT ecommerce.Product AS Item extra;", null);
    failedTables.recordUnparsed("Orders.total := ;", null);

    assertThat(isKnockOn("SELECT * FROM Customer", null)).isTrue();
    assertThat(isKnockOn("SELECT * FROM Item", null)).isTrue();
    assertThat(isKnockOn("SELECT id FROM Orders WHERE total > 10", null)).isTrue();
    assertThat(isKnockOn("SELECT * FROM Product", null)).isFalse();
  }

  @Test
  void givenFailedTableOfDatabase_whenUsedByQualifiedName_thenItsErrorIsKnockOn() {
    translator.executeSQL("CREATE DATABASE mydb");
    recordFailure("CREATE TABLE `mydb`.`Totals` (id INT) WITH ('connector' = 'x');", null);

    assertThat(isKnockOn("SELECT * FROM mydb.Totals", null)).isTrue();
    assertThat(isKnockOn("SELECT * FROM mydb.Others", null)).isFalse();
  }

  @Test
  void givenFailedColumnAndFunction_whenUsed_thenTheirErrorsAreKnockOn() {
    recordFailure("Orders.total := amount * missing;", null);
    recordFailure("CREATE FUNCTION myudf AS 'com.example.Missing';", null);

    assertThat(isKnockOn("SELECT id FROM Orders WHERE total > 10", null)).isTrue();
    assertThat(isKnockOn("SELECT o.total FROM Orders o", null)).isTrue();
    assertThat(isKnockOn("SELECT myudf(id) FROM Orders", null)).isTrue();
    assertThat(isKnockOn("SELECT otherudf(id) FROM Orders", null)).isFalse();
  }

  @Test
  void givenFailedTable_whenColumnIsAddedToItOrItIsExportedOrInsertedInto_thenItIsDependent() {
    recordFailure("Customer := SELECT * FROM CustomerUpdates;", null);
    recordFailure("CREATE TABLE Sink (id INT) WITH ('connector' = 'x');", null);

    assertThat(isDependent("Customer.total := amount * 2;")).isTrue();
    assertThat(isDependent("EXPORT Customer TO print.Customer;")).isTrue();
    assertThat(isDependent("EXPORT Orders TO Sink;")).isTrue();
    assertThat(isDependent("-- the sink\nINSERT INTO `Sink` SELECT id FROM Orders;")).isTrue();
    assertThat(isDependent("Orders.total := amount * 2;")).isFalse();
    assertThat(isDependent("EXPORT Orders TO print.Orders;")).isFalse();
  }

  @Test
  void givenFailedCatalog_whenUsed_thenItsErrorIsKnockOnAndTheContextFails() {
    recordFailure("CREATE CATALOG iceberg WITH ('type' = 'iceberg');", null);

    var error = catchThrowable(() -> translator.executeSQL("USE CATALOG iceberg"));
    assertThat(failedTables.isKnockOnError(error, null)).isTrue();
    assertThat(failedTables.isContextFailed()).isFalse();

    failedTables.recordSkipped(
        parser.parseStatement("USE CATALOG iceberg;"), "USE CATALOG iceberg;", null);

    // The statements after it would run in another catalog than intended
    assertThat(failedTables.isContextFailed()).isTrue();
  }

  @Test
  void givenFailedUseOfDatabase_whenContextIsRestored_thenItIsNotFailed() {
    recordFailure("/* the database */ USE my_database;", null);
    assertThat(failedTables.isContextFailed()).isTrue();

    // E.g. after an imported script, which has a context of its own
    failedTables.restoreContextFailed(false);

    assertThat(failedTables.isContextFailed()).isFalse();
  }

  private void recordFailure(String statement, String namespace) {
    failedTables.recordFailure(parser.parseStatement(statement), statement, false, namespace);
  }

  /** Whether the error of planning a view with the query is a knock-on error */
  private boolean isKnockOn(String query, String namespace) {
    var error =
        catchThrowable(
            () ->
                translator.addView(
                    "CREATE VIEW Q AS " + query, HintsAndDoc.EMPTY, ErrorCollector.root()));
    assertThat(error).as(query).isNotNull();
    return failedTables.isKnockOnError(error, namespace);
  }

  private boolean isDependent(String statement) {
    return failedTables.isDependent(parser.parseStatement(statement), statement, null);
  }
}
