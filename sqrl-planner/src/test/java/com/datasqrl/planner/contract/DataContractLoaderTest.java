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

import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    // The folder of the test is the build folder of the project
    loader = new DataContractLoader(errors, new WorkspacePaths(folder, folder, folder, folder));
  }

  @Test
  void givenScriptWithoutContractFile_whenForScript_thenEmptyWithoutMessage() {
    var contractFile = loader.forScript(file("plain.sqrl", ""));

    assertThat(contractFile).isEmpty();
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
    assertThat(loader.hasContractFiles()).isFalse();
  }

  @Test
  void givenSameScriptTwice_whenForScript_thenLoadsTheContractOnce() {
    file("crm.odcs.yaml", CONTRACT);
    var script = file("crm.sqrl", "");

    var first = loader.forScript(script);
    var second = loader.forScript(folder.resolve("../" + folder.getFileName() + "/crm.sqrl"));

    assertThat(second.orElseThrow()).isSameAs(first.orElseThrow());
    assertThat(first.get().getEntry("CUSTOMERS")).isPresent();
    assertThat(first.get().getEntry("Unknown")).isEmpty();
    assertThat(loader.getLoadedFiles()).hasSize(1);
  }

  @Test
  void givenTableSqlFile_whenForScript_thenPairsWithContractOfTheTableName() {
    file("orders.odcs.yaml", CONTRACT);

    var contractFile = loader.forScript(file("orders.table.sql", ""));

    assertThat(contractFile).isPresent();
    assertThat(contractFile.get().getPath().getFileName()).hasToString("orders.odcs.yaml");
  }

  @Test
  void givenFileThatItsFormatRejects_whenForScript_thenSaysOnceThatItIsNotApplied() {
    file("crm.odcs.yaml", CONTRACT.replace("v3.2.0", "v3.1.0"));
    var script = file("crm.sqrl", "");

    assertThat(loader.forScript(script)).isEmpty();
    assertThat(loader.forScript(script)).isEmpty();

    // The errors of the format in the file, then the one of the loader
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "apiVersion [v3.1.0] is not supported: convert the contract to ODCS v3.2.0",
            "Contract file [crm.odcs.yaml] is not a valid data contract and is not applied");
    assertThat(messages().get(1).getErrorLabel()).isEqualTo(ErrorCode.INVALID_DATA_CONTRACT);
    // The file was found, so it is not one that no script uses
    assertThat(loader.hasContractFiles()).isTrue();
    assertThat(loader.getSkippedFiles()).isEmpty();
    assertThat(loader.getLoadedFiles()).isEmpty();
  }

  @Test
  void givenContractNameDifferingInCase_whenForScript_thenReportsItOnEveryFileSystem() {
    file("Crm.odcs.yaml", CONTRACT);

    var contractFile = loader.forScript(file("crm.sqrl", ""));

    assertThat(contractFile).isEmpty();
    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .containsExactly(
            "Contract file [Crm.odcs.yaml] differs from the name of [crm.sqrl] only in case and is"
                + " not read. Rename it to [crm.odcs.yaml]");
  }

  @Test
  void givenScriptAndTableFileOfOneName_whenForScript_thenTheyShareTheContractFile() {
    file("orders.odcs.yaml", CONTRACT);

    var ofScript = loader.forScript(file("orders.sqrl", ""));
    var ofTableFile = loader.forScript(file("orders.table.sql", ""));

    assertThat(ofTableFile.orElseThrow()).isSameAs(ofScript.orElseThrow());
    assertThat(loader.getLoadedFiles()).hasSize(1);
    assertThat(errors.hasErrors()).isFalse();
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

  private List<ErrorMessage> messages() {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }
}
