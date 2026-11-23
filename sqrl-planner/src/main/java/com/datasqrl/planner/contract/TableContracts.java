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
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator.AddTableResult;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.contract.DataContractFile.AppliedTable;
import com.datasqrl.planner.contract.DataContractFormat.Entry;
import com.datasqrl.planner.util.Documented.Documentation;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * The tables that the {@code CREATE TABLE} statements of a compilation define, each with the data
 * contract that the project provides for it. The planner registers every table that it creates,
 * with the file whose statement defines it.
 *
 * <p>The contract of a table is the entry of the table's name, ignoring case, in the contract file
 * that sits next to the file that defines it. A table that takes over the columns of another table
 * with {@code LIKE} also finds its entry in the contract file next to the file that defines that
 * table, and so on along the {@code LIKE} clauses. That way a table that is defined once per
 * environment has one contract, next to the file with the columns that the environments share.
 */
@Component
@Lazy
public class TableContracts {

  /** The entry that a contract file provides for a table. */
  public record AttachedContract(DataContractFile file, Entry entry) {}

  /**
   * A created table, with what its statement declares besides its schema and its tags, which the
   * table analysis has.
   *
   * @param name the name of the table in its statement, without the suffix of an export and not an
   *     import alias
   * @param definingFile the file whose statement defines the table, if it is known
   * @param documentation the doc string of the table, with the comments of the columns that the doc
   *     string doesn't document
   * @param isTest whether the statement has the test hint
   * @param lookupFiles the files next to which the contract of the table is looked up: the file
   *     that defines it, then the files of the tables that it extends with {@code LIKE}
   */
  public record ContractTable(
      ObjectIdentifier id,
      TableAnalysis table,
      String name,
      Optional<Path> definingFile,
      Documentation documentation,
      boolean isTest,
      List<Path> lookupFiles,
      Optional<AttachedContract> contract) {

    /**
     * Identifies the statement: the same statement that is planned more than once, e.g. imported
     * under two names, is one definition.
     */
    public Object definition() {
      return definingFile.<Object>map(file -> new AppliedTable(file, name)).orElse(id);
    }
  }

  private final DataContractLoader loader;
  private final ErrorCollector errors;

  /** The created tables in the order they were planned */
  private final Map<ObjectIdentifier, ContractTable> tables = new LinkedHashMap<>();

  /** The contract of each definition, which is resolved, and reported on, once */
  private final Map<AppliedTable, Optional<AttachedContract>> resolved = new HashMap<>();

  public TableContracts(DataContractLoader loader, ErrorCollector errors) {
    this.loader = loader;
    this.errors = errors.abortOnFatal(false);
  }

  /**
   * @param created a table that the planner created
   * @param definingFile the file whose statement defines the table, a script or a {@code
   *     .table.sql} file; a table of an unknown file has no contract
   * @param extendedTable the table whose columns the statement takes over with {@code LIKE}
   */
  public ContractTable register(
      AddTableResult created,
      Optional<Path> definingFile,
      Optional<ObjectIdentifier> extendedTable) {
    var file = definingFile.map(path -> path.toAbsolutePath().normalize());
    var lookupFiles =
        Stream.concat(
                file.stream(),
                extendedTable.map(tables::get).map(ContractTable::lookupFiles).stream()
                    .flatMap(List::stream))
            .distinct()
            .toList();
    var contract =
        file.flatMap(
            path -> {
              var definition = new AppliedTable(path, created.statementName());
              var known = resolved.get(definition);
              if (known == null) {
                known = resolve(definition, lookupFiles);
                resolved.put(definition, known);
              }
              return known;
            });

    var table =
        new ContractTable(
            created.baseTableIdentifier(),
            created.tableAnalysis(),
            created.statementName(),
            file,
            created.documentation(),
            created.hintsAndDoc().hints().isTest(),
            lookupFiles,
            contract);
    tables.put(table.id(), table);
    return table;
  }

  public Optional<ContractTable> get(ObjectIdentifier tableId) {
    return Optional.ofNullable(tables.get(tableId));
  }

  /** The created tables, in the order they were planned. */
  public Collection<ContractTable> getTables() {
    return Collections.unmodifiableCollection(tables.values());
  }

  private Optional<AttachedContract> resolve(AppliedTable table, List<Path> lookupFiles) {
    var own = attach(table.definingFile(), table);
    var inherited =
        lookupFiles.stream()
            .filter(file -> !file.equals(table.definingFile()))
            .flatMap(file -> attach(file, table).stream())
            .findFirst();
    if (own.isPresent() && inherited.isPresent()) {
      reportTwoContracts(own.get(), inherited.get());
    }
    return own.or(() -> inherited);
  }

  /**
   * @param script a file that defines tables
   * @return the entry for the table in the contract file next to the script, if it has one
   */
  private Optional<AttachedContract> attach(Path script, AppliedTable table) {
    return loader
        .forScript(script)
        .flatMap(
            file -> file.getEntry(table.tableName()).flatMap(entry -> apply(file, entry, table)));
  }

  private Optional<AttachedContract> apply(DataContractFile file, Entry entry, AppliedTable table) {
    var applied = file.markApplied(entry, table);
    if (!applied.equals(table)) {
      var label = file.getFormat().getEntryLabel();
      // The match is ambiguous, so neither the check nor the output can pick a table
      file.errorsAt(errors, entry.location())
          .fatal(
              ErrorCode.INVALID_DATA_CONTRACT,
              "%s [%s] matches more than one table: [%s] in %s and [%s] in %s. A %s describes one"
                  + " table",
              StringUtils.capitalize(label),
              entry.name(),
              applied.tableName(),
              applied.definingFile().getFileName(),
              table.tableName(),
              table.definingFile().getFileName(),
              label);
      return Optional.empty();
    }
    return Optional.of(new AttachedContract(file, entry));
  }

  private void reportTwoContracts(AttachedContract own, AttachedContract inherited) {
    own.file()
        .errorsAt(errors, own.entry().location())
        .fatal(
            ErrorCode.INVALID_DATA_CONTRACT,
            "Table [%s] has a %s in two contract files: here, next to its script, and in [%s]"
                + " next to the script of a table it extends. Keep one of them",
            own.entry().name(),
            own.file().getFormat().getEntryLabel(),
            inherited.file().getPath().getFileName());
  }
}
