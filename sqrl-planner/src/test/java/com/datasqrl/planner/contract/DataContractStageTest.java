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
import com.datasqrl.planner.contract.DataContracts.ContractFileUsage;
import com.datasqrl.planner.tags.TagDefinition;
import com.datasqrl.planner.tags.TagDefinitions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
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
  private DataContractStage stage;

  @BeforeEach
  void setUp() throws Exception {
    buildDir = Files.createDirectories(workspace.resolve("build")).toRealPath();
    loader = new DataContractLoader(errors);
    stage =
        new DataContractStage(
            loader,
            new WorkspacePaths(workspace, buildDir, buildDir.resolve("deploy"), workspace),
            null,
            errors);
  }

  @Test
  void givenAppliedAndUnusedContractFiles_whenCheck_thenListsHowEachWasUsed() {
    file("connectors/crm.odcs.yaml", CONTRACT);
    loader.forFile(file("connectors/crm.sqrl", "")).forTable("Customers");
    // The script next to this contract file is not part of the compilation
    file("sinks/archive-prod.odcs.yaml", CONTRACT);
    file("sinks/archive-prod.sqrl", "");

    var usages = stage.checkContractFiles(DEFINITIONS, errors.abortOnFatal(false));

    assertThat(usages)
        .extracting(usage -> usage.describe(buildDir.relativize(usage.file())))
        .containsExactly(
            "connectors/crm.odcs.yaml: Customers (not applied: Invoices)",
            "sinks/archive-prod.odcs.yaml: not used in this compile");
    assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
  }

  @Test
  void givenLoadedContractFileWithoutAppliedObject_whenCheck_thenReportsIt() {
    file("crm.odcs.yaml", CONTRACT);
    loader.forFile(file("crm.sqrl", "")).forTable("Custmers");

    var usages = stage.checkContractFiles(DEFINITIONS, errors.abortOnFatal(false));

    assertThat(usages)
        .extracting(ContractFileUsage::used, ContractFileUsage::unappliedObjects)
        .containsExactly(tuple(true, List.of("Customers", "Invoices")));
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
    loader.forFile(file("crm.sqrl", "")).forTable("Customers");
    file("plain.odcs.yaml", CONTRACT.replace("    tags: [pii=raw]\n", ""));
    loader.forFile(file("plain.sqrl", "")).forTable("Customers");

    stage.checkContractFiles(TagDefinitions.DISABLED, errors.abortOnFatal(false));

    assertThat(messages())
        .extracting(ErrorMessage::getMessage, ErrorMessage::isWarning)
        .containsExactly(
            tuple(
                "Contract file [crm.odcs.yaml] declares tags or a classification, which are not"
                    + " compared with the tables since compiler.tags is not configured",
                true));
  }

  @Test
  void givenInvalidContractFile_whenCheck_thenItIsNotListedAndNotReportedAgain() {
    file("crm.odcs.yaml", "apiVersion: [v3.2.0");
    loader.forFile(file("crm.sqrl", "")).forTable("Customers");
    var reported = messages().size();

    var usages = stage.checkContractFiles(DEFINITIONS, errors.abortOnFatal(false));

    assertThat(usages).isEmpty();
    assertThat(messages()).hasSize(reported);
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
