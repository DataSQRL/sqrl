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

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.contract.DataContractFile.AppliedTable;
import com.datasqrl.planner.contract.TableContract.AttachedContract;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

/** The contract source of a file that defines tables, with the contract file next to it. */
@RequiredArgsConstructor
final class FileContractSource implements ContractSource {

  private final DataContractLoader loader;
  private final Path definingFile;

  /** The contract file next to the defining file, empty if there is none or it is invalid */
  private final Optional<DataContractFile> contractFile;

  private final ErrorCollector errors;

  /** The contracts handed out, so that the same definition always gets the same contract */
  private final Map<AppliedTable, TableContract> contracts = new HashMap<>();

  @Override
  public TableContract forTable(
      String tableName, boolean isSchemaTable, Optional<TableContract> extended) {
    var own = contractOf(definingFile, tableName);
    if (isSchemaTable) {
      // The tables that extend a schema table look in the contract file of this file
      return own.withSchemaTableFile(Optional.of(definingFile));
    }

    var schemaTableFile = extended.flatMap(TableContract::schemaTableFile);
    var inherited =
        schemaTableFile
            .filter(file -> !file.equals(definingFile))
            .flatMap(file -> schemaTableSource(file).inheritedContract(definingFile, tableName));
    if (inherited.isPresent() && own.attached().isPresent()) {
      reportTwoContracts(own.attached().get(), inherited.get().attached().orElseThrow());
    }
    return (own.attached().isPresent() ? own : inherited.orElse(own))
        .withSchemaTableFile(schemaTableFile);
  }

  private FileContractSource schemaTableSource(Path schemaTableFile) {
    // The source of a file path is always a file source
    return (FileContractSource) loader.forFile(schemaTableFile);
  }

  /**
   * @param tableFile the file that defines a table that extends a schema table of this file
   * @return the contract of the table from the contract file of this file, if it has a schema
   *     object for the table
   */
  private Optional<TableContract> inheritedContract(Path tableFile, String tableName) {
    return Optional.of(contractOf(tableFile, tableName))
        .filter(contract -> contract.attached().isPresent());
  }

  private TableContract contractOf(Path tableFile, String tableName) {
    return contracts.computeIfAbsent(
        new AppliedTable(tableFile, tableName),
        table ->
            new TableContract(
                Optional.of(table.definingFile()),
                table.tableName(),
                contractFile.flatMap(file -> attach(file, table)),
                Optional.empty()));
  }

  private Optional<AttachedContract> attach(DataContractFile file, AppliedTable table) {
    var schemaObject = file.getSchemaObject(table.tableName());
    if (schemaObject.isEmpty()) {
      return Optional.empty();
    }
    var object = schemaObject.get();
    var applied = file.markApplied(object, table);
    if (!applied.equals(table)) {
      // The match is ambiguous, so neither the check nor the output can pick a table
      file.errorsAt(errors, object.pointer())
          .fatal(
              ErrorCode.INVALID_DATA_CONTRACT,
              "Schema object [%s] matches more than one table: [%s] in %s and [%s] in %s."
                  + " A schema object describes one table",
              object.name(),
              applied.tableName(),
              applied.definingFile().getFileName(),
              table.tableName(),
              table.definingFile().getFileName());
      return Optional.empty();
    }
    return Optional.of(new AttachedContract(file, object.pointer(), object.node()));
  }

  private void reportTwoContracts(AttachedContract own, AttachedContract inherited) {
    own.file()
        .errorsAt(errors, own.pointer())
        .fatal(
            ErrorCode.INVALID_DATA_CONTRACT,
            "Table [%s] has a schema object in two contract files: here, next to its script, and"
                + " in [%s] next to the script of the schema table it extends. Keep one of them",
            own.schemaObject().path("name").asText(),
            inherited.file().getPath().getFileName());
  }
}
