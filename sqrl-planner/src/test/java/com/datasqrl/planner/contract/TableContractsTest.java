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
package com.datasqrl.planner.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.AddTableResult;
import com.datasqrl.planner.contract.TableContracts.ContractTable;
import com.datasqrl.planner.hint.HintsAndDoc;
import com.datasqrl.planner.util.Documented;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableContractsTest {

  private static final String CONTRACT =
      """
      apiVersion: v3.2.0
      kind: DataContract
      id: crm
      version: 1.0.0
      schema:
        - name: Customers
          properties:
            - name: id
              logicalType: integer
        - name: Orders
      """;

  @TempDir Path folder;

  private final ErrorCollector errors = ErrorCollector.root();
  private DataContractLoader loader;
  private TableContracts underTest;
  private int tables;

  @BeforeEach
  void setUp() {
    loader = new DataContractLoader(errors, new WorkspacePaths(folder, folder, folder, folder));
    underTest = new TableContracts(loader);
  }

  @Test
  void givenContractNextToTheScript_whenRegister_thenAttachesTheEntryIgnoringCase() {
    file("crm.odcs.yaml", CONTRACT);
    var script = file("crm.sqrl", "");

    var table = register("CUSTOMERS", script);

    var attached = table.contract().orElseThrow();
    assertThat(attached.entry().name()).isEqualTo("Customers");
    assertThat(attached.entry().location().line()).isEqualTo(6);
    assertThat(register("Unknown", script).contract()).isEmpty();
    assertThat(attached.file().getAppliedTableNames()).containsExactly("CUSTOMERS");
    assertThat(attached.file().getUnappliedEntryNames()).containsExactly("Orders");
    assertThat(underTest.get(table.id())).contains(table);
    assertThat(underTest.getTables()).hasSize(2);
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenStatementThatIsPlannedTwice_whenRegister_thenItIsOneDefinitionWithTheContract() {
    file("crm.odcs.yaml", CONTRACT);
    var script = file("crm.sqrl", "");

    var first = register("Customers", script);
    var second = register("Customers", script);

    assertThat(second.id()).isNotEqualTo(first.id());
    assertThat(second.definition()).isEqualTo(first.definition());
    assertThat(second.contract()).isPresent().isEqualTo(first.contract());
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenTableOfAnUnknownFile_whenRegister_thenItHasNoContract() {
    var table = underTest.register(created("Customers"), Optional.empty(), Optional.empty());

    assertThat(table.contract()).isEmpty();
    assertThat(table.definition()).isEqualTo(table.id());
    assertThat(loader.getLoadedFiles()).isEmpty();
  }

  @Test
  void givenTableThatExtendsATableOfAnotherFile_whenRegister_thenFindsTheContractNextToThatFile() {
    file("crm.odcs.yaml", CONTRACT);
    var schemaTable = register("_Customers_schema", file("crm.sqrl", ""));
    var environmentFile = file("crm-prod.sqrl", "");

    var table = register("Customers", environmentFile, schemaTable);

    assertThat(schemaTable.contract()).isEmpty();
    assertThat(table.contract()).isPresent();
    assertThat(table.definingFile()).contains(environmentFile);
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenChainOfExtendedTables_whenRegister_thenFollowsIt() {
    file("crm.odcs.yaml", CONTRACT);
    // The names of the tables play no part
    var base = register("CustomerColumns", file("crm.sqrl", ""));
    var shared = register("Shared", file("crm-shared.sqrl", ""), base);

    var table = register("Orders", file("crm-prod.sqrl", ""), shared);

    assertThat(shared.contract()).isEmpty();
    assertThat(table.contract()).isPresent();
  }

  @Test
  void givenEntryNextToTheTableAndNextToAnExtendedTable_whenRegister_thenReportsBothFiles() {
    file("crm.odcs.yaml", CONTRACT);
    file("crm-prod.odcs.yaml", CONTRACT);
    var schemaTable = register("_Customers_schema", file("crm.sqrl", ""));

    var table = register("Customers", file("crm-prod.sqrl", ""), schemaTable);

    assertThat(table.contract().orElseThrow().file().getPath().getFileName())
        .hasToString("crm-prod.odcs.yaml");
    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(
            tuple(
                "Table [Customers] has a schema object in two contract files: here, next to its"
                    + " script, and in [crm.odcs.yaml] next to the script of a table it extends."
                    + " Keep one of them",
                6));
    // Both files have an entry for the table, so neither is reported as not applied
    assertThat(loader.getLoadedFiles())
        .allSatisfy(file -> assertThat(file.getAppliedTableNames()).containsExactly("Customers"));
  }

  @Test
  void givenTwoTablesDifferingInCase_whenRegister_thenReportsTheAmbiguousMatch() {
    file("crm.odcs.yaml", CONTRACT);
    var script = file("crm.sqrl", "");

    var first = register("Customers", script);
    var second = register("customers", script);

    assertThat(first.contract()).isPresent();
    assertThat(second.contract()).isEmpty();
    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(
            tuple(
                "Schema object [Customers] matches more than one table: [Customers] in crm.sqrl"
                    + " and [customers] in crm.sqrl. A schema object describes one table",
                6));
  }

  @Test
  void givenScriptAndTableFileOfOneName_whenRegister_thenAnEntryDescribesATableOfOneOfThem() {
    file("orders.odcs.yaml", CONTRACT);
    var script = file("orders.sqrl", "");
    var tableFile = file("orders.table.sql", "");

    assertThat(register("Customers", script).contract()).isPresent();
    assertThat(register("Orders", tableFile).contract()).isPresent();
    assertThat(errors.hasErrors()).isFalse();

    assertThat(register("Customers", tableFile).contract()).isEmpty();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Schema object [Customers] matches more than one table: [Customers] in orders.sqrl"
                + " and [Customers] in orders.table.sql. A schema object describes one table");
  }

  private ContractTable register(String name, Path definingFile) {
    return underTest.register(created(name), Optional.of(definingFile), Optional.empty());
  }

  /** Registers a table that extends another one with LIKE. */
  private ContractTable register(String name, Path definingFile, ContractTable extended) {
    return underTest.register(created(name), Optional.of(definingFile), Optional.of(extended.id()));
  }

  /** A created table as the translator returns it, with what the registration reads. */
  private AddTableResult created(String name) {
    return new AddTableResult(
        name,
        name,
        ObjectIdentifier.of("catalog", "database", name + "_" + tables++),
        true,
        null,
        null,
        "",
        HintsAndDoc.EMPTY,
        Documented.EMPTY,
        Optional.empty());
  }

  @SneakyThrows
  private Path file(String name, String content) {
    var path = folder.resolve(name);
    Files.writeString(path, content);
    return path.toAbsolutePath().normalize();
  }

  private List<ErrorMessage> messages() {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }
}
