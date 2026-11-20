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

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataContractLoaderTest {

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

  @BeforeEach
  void setUp() {
    loader = new DataContractLoader(errors);
  }

  @Test
  void givenScriptWithoutContractFile_whenForTable_thenAttachesNothingWithoutMessage() {
    var source = loader.forFile(file("plain.sqrl", ""));

    var contract = source.forTable("Customers");

    assertThat(contract.attached()).isEmpty();
    assertThat(contract.tableName()).isEqualTo("Customers");
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
    assertThat(loader.getFoundFiles()).isEmpty();
  }

  @Test
  void givenContractFile_whenForTable_thenAttachesSchemaObjectIgnoringCase() {
    file("crm.odcs.yaml", CONTRACT);
    var source = loader.forFile(file("crm.sqrl", ""));

    var contract = source.forTable("CUSTOMERS");

    var attached = contract.attached().orElseThrow();
    assertThat(attached.pointer()).isEqualTo("/schema/0");
    assertThat(attached.schemaObject().get("name").asText()).isEqualTo("Customers");
    assertThat(attached.file().getVersion()).isEqualTo(OdcsVersion.V3_2);
    assertThat(source.forTable("Unknown").attached()).isEmpty();
    assertThat(source.forTable("CUSTOMERS")).isEqualTo(contract);
    assertThat(attached.file().getAppliedTableNames()).containsExactly("CUSTOMERS");
    assertThat(attached.file().getUnappliedObjectNames()).containsExactly("Orders");
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenSameScriptTwice_whenForFile_thenLoadsTheContractOnce() {
    file("crm.odcs.yaml", CONTRACT);
    var script = file("crm.sqrl", "");

    var first = loader.forFile(script);
    var second = loader.forFile(folder.resolve("../" + folder.getFileName() + "/crm.sqrl"));

    assertThat(second).isSameAs(first);
    assertThat(loader.getLoadedFiles()).hasSize(1);
  }

  @Test
  void givenTableSqlFile_whenForFile_thenPairsWithContractOfTheTableName() {
    file("orders.odcs.yaml", CONTRACT);

    var contract = loader.forFile(file("orders.table.sql", "")).forTable("Orders");

    assertThat(contract.attached()).isPresent();
  }

  @Test
  void givenTableThatExtendsSchemaTable_whenForTable_thenFindsContractOfSchemaTableFile() {
    file("crm.odcs.yaml", CONTRACT);
    var base = loader.forFile(file("crm.sqrl", ""));
    var environmentFile = file("crm-prod.sqrl", "");

    var schemaTable = base.forTable("_Customers_schema", true, Optional.empty());
    var contract =
        loader.forFile(environmentFile).forTable("Customers", false, Optional.of(schemaTable));

    assertThat(schemaTable.attached()).isEmpty();
    assertThat(contract.attached()).isPresent();
    assertThat(contract.definingFile()).contains(environmentFile);
    assertThat(contract.schemaTableFile()).isEqualTo(schemaTable.definingFile());
    assertThat(errors.hasErrors()).isFalse();
  }

  @Test
  void givenTableThatExtendsTableOfSchemaTable_whenForTable_thenFollowsTheChain() {
    file("crm.odcs.yaml", CONTRACT);
    var schemaTable =
        loader.forFile(file("crm.sqrl", "")).forTable("_Customers_schema", true, Optional.empty());
    var shared =
        loader
            .forFile(file("crm-shared.sqrl", ""))
            .forTable("_Shared", false, Optional.of(schemaTable));

    var contract =
        loader.forFile(file("crm-prod.sqrl", "")).forTable("Orders", false, Optional.of(shared));

    assertThat(shared.attached()).isEmpty();
    assertThat(contract.attached()).isPresent();
  }

  @Test
  void givenSchemaObjectNextToTableAndNextToSchemaTable_whenForTable_thenReportsBothFiles() {
    file("crm.odcs.yaml", CONTRACT);
    file("crm-prod.odcs.yaml", CONTRACT);
    var schemaTable =
        loader.forFile(file("crm.sqrl", "")).forTable("_Customers_schema", true, Optional.empty());

    var contract =
        loader
            .forFile(file("crm-prod.sqrl", ""))
            .forTable("Customers", false, Optional.of(schemaTable));

    assertThat(contract.attached().orElseThrow().file().getPath().getFileName())
        .hasToString("crm-prod.odcs.yaml");
    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(
            tuple(
                "Table [Customers] has a schema object in two contract files: here, next to its"
                    + " script, and in [crm.odcs.yaml] next to the script of the schema table it"
                    + " extends. Keep one of them",
                6));
    // Both files have a schema object for the table, so neither is reported as not applied
    assertThat(loader.getLoadedFiles())
        .allSatisfy(file -> assertThat(file.getAppliedTableNames()).containsExactly("Customers"));
  }

  @Test
  void givenYamlSyntaxError_whenForFile_thenReportsItOnceWithItsLine() {
    file("crm.odcs.yaml", "apiVersion: v3.2.0\nschema:\n  - name: [unclosed\n");

    var source = loader.forFile(file("crm.sqrl", ""));

    assertThat(source.forTable("Customers").attached()).isEmpty();
    assertThat(fileErrors()).hasSize(1);
    assertThat(fileErrors().get(0).getMessage()).startsWith("Invalid YAML:");
    assertThat(fileErrors().get(0).getLocation().getFileLocation().line())
        .isGreaterThanOrEqualTo(3);
  }

  @Test
  void givenSchemaViolations_whenForFile_thenReportsEachWithItsLine() {
    file(
        "crm.odcs.yaml",
        """
        apiVersion: v3.2.0
        kind: DataContract
        id: crm
        version: 1.0
        schema:
          - name: Customers
            columns: []
            properties:
              - name: id
                logicalType: int
              - name: email
                required: "yes"
        """);

    var source = loader.forFile(file("crm.sqrl", ""));

    assertThat(source.forTable("Customers").attached()).isEmpty();
    assertThat(fileErrors())
        .extracting(message -> message.getLocation().getFileLocation().line())
        .contains(4, 10, 12);
    assertThat(fileErrors())
        .filteredOn(message -> message.getLocation().getFileLocation().line() == 4)
        .extracting(ErrorMessage::getMessage)
        .allSatisfy(message -> assertThat(message).endsWith("Quote the value to make it a string"));
  }

  @Test
  void givenSchemaThatIsNotAList_whenForFile_thenReportsItWithoutFailing() {
    file(
        "crm.odcs.yaml",
        """
        apiVersion: v3.2.0
        kind: DataContract
        id: crm
        version: 1.0.0
        status: active
        schema:
          name: Customers
        """);

    var source = loader.forFile(file("crm.sqrl", ""));

    assertThat(source.forTable("Customers").attached()).isEmpty();
    assertThat(fileErrors())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(tuple("schema: object found, array expected", 7));
  }

  @Test
  void givenContractThatIsAList_whenForFile_thenSaysWhatAContractIs() {
    file("crm.odcs.yaml", "- name: Customers\n");

    loader.forFile(file("crm.sqrl", ""));

    assertThat(fileErrors())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "The contract file does not hold a data contract: it must be a YAML mapping with the"
                + " fields of ODCS, such as apiVersion and schema");
  }

  @Test
  void givenFieldThatOdcsDoesNotDefine_whenForFile_thenReportsOnlyThatField() {
    // The validator reports every field of an object with an unknown field as not evaluated
    file(
        "crm.odcs.yaml",
        CONTRACT.replace("        logicalType: integer\n", "        nullable: true\n"));

    loader.forFile(file("crm.sqrl", ""));

    assertThat(fileErrors())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(tuple("nullable: not a field that ODCS defines for this element", 9));
  }

  @Test
  void givenViolationsInTheSecondAndTwelfthProperty_whenForFile_thenReportsBoth() {
    var properties = new StringBuilder();
    for (var i = 0; i < 11; i++) {
      properties.append("      - name: column").append(i).append("\n");
    }
    // The second property has a field of another element, which is only reported as not
    // evaluated. The violation of the twelfth property, whose pointer starts alike, is no reason
    // to leave that out
    file(
        "crm.odcs.yaml",
        CONTRACT
            .replace(
                "        logicalType: integer\n", "        logicalType: integer\n" + properties)
            .replace("      - name: column0\n", "      - name: column0\n        kind: Customer\n")
            .replace(
                "      - name: column10\n", "      - name: column10\n        logicalType: int\n"));

    loader.forFile(file("crm.sqrl", ""));

    assertThat(fileErrors())
        .extracting(ErrorMessage::getMessage)
        .anySatisfy(message -> assertThat(message).startsWith("kind: "))
        .anySatisfy(message -> assertThat(message).startsWith("logicalType: "));
  }

  @Test
  void givenVersion2Contract_whenForFile_thenAsksToConvertIt() {
    file("crm.odcs.yaml", CONTRACT.replace("v3.2.0", "v2.2.2"));

    loader.forFile(file("crm.sqrl", ""));

    assertThat(fileErrors())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .contains(
            tuple("apiVersion [v2.2.2] is not supported: convert the contract to ODCS v3", 1));
  }

  @Test
  void givenSchemaObjectsDifferingInCase_whenForFile_thenReportsTheSecond() {
    file("crm.odcs.yaml", CONTRACT + "  - name: orders\n");

    var source = loader.forFile(file("crm.sqrl", ""));

    assertThat(source.forTable("Orders").attached()).isEmpty();
    assertThat(fileErrors())
        .extracting(
            ErrorMessage::getMessage, message -> message.getLocation().getFileLocation().line())
        .containsExactly(
            tuple(
                "Schema objects [Orders] and [orders] differ only in case. Table names are"
                    + " matched ignoring case",
                11));
  }

  @Test
  void givenContractNameDifferingInCase_whenForFile_thenReportsItOnEveryFileSystem() {
    file("Crm.odcs.yaml", CONTRACT);

    var source = loader.forFile(file("crm.sqrl", ""));

    assertThat(source.forTable("Customers").attached()).isEmpty();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Contract file [Crm.odcs.yaml] differs from the name of [crm.sqrl] only in case and is"
                + " not read. Rename it to [crm.odcs.yaml]");
  }

  @Test
  void givenTwoYamlDocuments_whenForFile_thenReportsIt() {
    file("crm.odcs.yaml", CONTRACT + "---\n" + CONTRACT);

    var source = loader.forFile(file("crm.sqrl", ""));

    assertThat(source.forTable("Customers").attached()).isEmpty();
    assertThat(fileErrors())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "The contract file has 2 YAML documents. A contract file holds one document");
  }

  @Test
  void givenTwoTablesDifferingInCase_whenForTable_thenReportsTheAmbiguousMatch() {
    file("crm.odcs.yaml", CONTRACT);
    var source = loader.forFile(file("crm.sqrl", ""));

    var first = source.forTable("Customers");
    var second = source.forTable("customers");

    assertThat(first.attached()).isPresent();
    assertThat(second.attached()).isEmpty();
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
  void givenScriptAndTableFileOfOneName_whenForTable_thenTheyShareTheContract() {
    file("orders.odcs.yaml", CONTRACT);
    var script = loader.forFile(file("orders.sqrl", ""));
    var tableFile = loader.forFile(file("orders.table.sql", ""));

    assertThat(script.forTable("Customers").attached()).isPresent();
    assertThat(tableFile.forTable("Orders").attached()).isPresent();
    assertThat(loader.getLoadedFiles()).hasSize(1);
    assertThat(errors.hasErrors()).isFalse();

    assertThat(tableFile.forTable("Customers").attached()).isEmpty();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Schema object [Customers] matches more than one table: [Customers] in orders.sqrl"
                + " and [Customers] in orders.table.sql. A schema object describes one table");
  }

  @Test
  void givenFileThatCannotDefineTables_whenStemOf_thenEmpty() {
    assertThat(DataContractLoader.stemOf("crm.sqrl")).contains("crm");
    assertThat(DataContractLoader.stemOf("orders.table.sql")).contains("orders");
    assertThat(DataContractLoader.stemOf("schema.graphqls")).isEmpty();
  }

  @SneakyThrows
  private Path file(String name, String content) {
    var path = folder.resolve(name);
    Files.writeString(path, content);
    return path.toAbsolutePath().normalize();
  }

  /**
   * The errors of an invalid contract file, without the last error, which says once that the file
   * is not applied.
   */
  private List<ErrorMessage> fileErrors() {
    var all = messages();
    assertThat(all.get(all.size() - 1))
        .extracting(ErrorMessage::getMessage, ErrorMessage::getErrorLabel)
        .containsExactly(
            "Contract file [crm.odcs.yaml] is not a valid data contract and is not applied",
            ErrorCode.INVALID_DATA_CONTRACT);
    return all.subList(0, all.size() - 1);
  }

  private List<ErrorMessage> messages() {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }
}
