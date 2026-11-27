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
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.AddTableResult;
import com.datasqrl.planner.hint.HintsAndDoc;
import com.datasqrl.planner.tags.TagDefinition;
import com.datasqrl.planner.tags.TagDefinitions;
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

/** Tests how the stage reports the contract files of a project. */
class DataContractStageTest {

  private static final String CONTRACT =
      """
      apiVersion: v3.2.0
      kind: DataContract
      id: crm
      version: 1.0.0
      status: active
      schema:
        - name: Customers
          properties:
            - name: id
              logicalType: integer
        - name: Invoices
          tags: [pii=raw]
      """;

  private static final TagDefinitions DEFINITIONS =
      TagDefinitions.of(List.of(TagDefinition.of("pii", List.of("masked", "raw"), List.of())));

  @TempDir Path workspace;

  private final ErrorCollector errors = ErrorCollector.root();
  private Path buildDir;
  private DataContractLoader loader;
  private TableContracts tableContracts;
  private DataContractStage stage;
  private int tables;

  @BeforeEach
  void setUp() throws Exception {
    buildDir = Files.createDirectories(workspace.resolve("build")).toRealPath();
    loader = new DataContractLoader(errors);
    tableContracts = new TableContracts(loader, errors);
    stage =
        new DataContractStage(
            loader,
            tableContracts,
            new WorkspacePaths(workspace, buildDir, buildDir.resolve("deploy"), workspace),
            null,
            errors);
  }

  @Test
  void givenEntryWithoutTableAndSkippedFile_whenCheck_thenNoticesTheEntryAndWarnsAboutTheFile() {
    file("connectors/crm.odcs.yaml", CONTRACT);
    register("Customers", file("connectors/crm.sqrl", ""));
    // The script next to this contract file is not part of the compilation
    file("sinks/archive-prod.odcs.yaml", CONTRACT);
    file("sinks/archive-prod.sqrl", "");

    stage.checkContractFiles(DEFINITIONS, errors.abortOnFatal(false));

    assertThat(messages())
        .extracting(ErrorMessage::getMessage, DataContractStageTest::severity)
        .containsExactly(
            tuple("Schema object [Invoices] matches no table and is not checked", "notice"),
            tuple(
                "Contract file [sinks/archive-prod.odcs.yaml] is skipped: the script next to it"
                    + " is not part of this compile",
                "warning"));
    // The notice points at the schema object in its file
    assertThat(messages().get(0).getLocation().getFileLocation().line()).isEqualTo(11);
  }

  @Test
  void givenSkippedFilesInSeveralFolders_whenCheck_thenWarnsInTheOrderOfTheirPaths() {
    for (var name : List.of("sinks/b", "a", "sinks/a", "connectors/z")) {
      file(name + ".odcs.yaml", CONTRACT);
      file(name + ".sqrl", "");
    }

    stage.checkContractFiles(DEFINITIONS, errors.abortOnFatal(false));

    assertThat(messages())
        .extracting(ErrorMessage::getMessage)
        .extracting(message -> message.substring(0, message.indexOf(']') + 1))
        .containsExactly(
            "Contract file [a.odcs.yaml]",
            "Contract file [connectors/z.odcs.yaml]",
            "Contract file [sinks/a.odcs.yaml]",
            "Contract file [sinks/b.odcs.yaml]");
  }

  @Test
  void givenLoadedContractFileWithoutAppliedObject_whenCheck_thenReportsItOnce() {
    file("crm.odcs.yaml", CONTRACT);
    register("Custmers", file("crm.sqrl", ""));

    stage.checkContractFiles(DEFINITIONS, errors.abortOnFatal(false));

    // An error for the file, and no notice for each of its schema objects
    assertThat(messages())
        .extracting(
            ErrorMessage::getMessage,
            ErrorMessage::getErrorLabel,
            message -> message.getLocation().getFileLocation().line())
        .containsExactly(
            tuple(
                "Contract file [crm.odcs.yaml] was not applied: none of its schema objects"
                    + " [Customers, Invoices] matches a table",
                ErrorCode.INVALID_DATA_CONTRACT,
                7));
  }

  @Test
  void givenTagsInContractWithoutTagDefinitions_whenCheck_thenWarnsOncePerFile() {
    file("crm.odcs.yaml", CONTRACT);
    register("Customers", file("crm.sqrl", ""));
    file("plain.odcs.yaml", CONTRACT.replace("    tags: [pii=raw]\n", ""));
    register("Customers", file("plain.sqrl", ""));

    stage.checkContractFiles(TagDefinitions.DISABLED, errors.abortOnFatal(false));

    assertThat(messages())
        .filteredOn(ErrorMessage::isWarning)
        .extracting(ErrorMessage::getMessage, ErrorMessage::isWarning)
        .containsExactly(
            tuple(
                "Contract file [crm.odcs.yaml] declares tags or a classification, which are not"
                    + " compared with the tables since compiler.tags is not configured",
                true));
  }

  @Test
  void givenInvalidContractFile_whenCheck_thenItIsNotReportedAgain() {
    file("crm.odcs.yaml", "apiVersion: [v3.2.0");
    register("Customers", file("crm.sqrl", ""));
    var reported = messages().size();

    stage.checkContractFiles(DEFINITIONS, errors.abortOnFatal(false));

    // The file was found, so it is not skipped either
    assertThat(messages()).hasSize(reported);
  }

  /** Registers a table that a statement of the script creates. */
  private void register(String name, Path script) {
    tableContracts.register(
        new AddTableResult(
            name,
            name,
            ObjectIdentifier.of("catalog", "database", name + "_" + tables++),
            true,
            null,
            null,
            "",
            HintsAndDoc.EMPTY,
            Documented.EMPTY,
            Optional.empty()),
        Optional.of(script),
        Optional.empty());
  }

  private static String severity(ErrorMessage message) {
    return message.isNotice() ? "notice" : message.isWarning() ? "warning" : "fatal";
  }

  @SneakyThrows
  private Path file(String name, String content) {
    var path = buildDir.resolve(name);
    Files.createDirectories(path.getParent());
    Files.writeString(path, content);
    return path;
  }

  private List<ErrorMessage> messages() {
    var result = new ArrayList<ErrorMessage>();
    errors.forEach(result::add);
    return result;
  }
}
