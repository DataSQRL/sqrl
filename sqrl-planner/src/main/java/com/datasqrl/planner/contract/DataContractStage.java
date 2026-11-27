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

import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.MainScript;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.contract.DataContractFormat.GeneratedEntry;
import com.datasqrl.planner.contract.TableContracts.AttachedContract;
import com.datasqrl.planner.contract.TableContracts.ContractTable;
import com.datasqrl.planner.dag.PipelineDAG;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.tables.StorageType;
import com.datasqrl.planner.tags.TagDefinitions;
import com.datasqrl.util.StreamUtil;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Checks the data contracts that the project provides against its tables, and assembles the data
 * contracts of the deployment:
 *
 * <ul>
 *   <li>{@code sources} and {@code sinks} describe the tables that the deployed pipeline reads and
 *       writes. A table has the entry that a contract file provides for it, and a generated one
 *       otherwise.
 *   <li>The contract named after the main script describes the public tables, see {@link
 *       PublicTables}. They are always generated.
 * </ul>
 *
 * A contract file next to a planned script is either applied or reported, so that a contract that
 * the compiler does not pick up is not mistaken for one that passed. How a contract is read,
 * compared and written is up to its {@link DataContractFormat}.
 */
@Component
@Lazy
@RequiredArgsConstructor
public class DataContractStage {

  /** The name of the contract with the tables that the pipeline reads */
  public static final String SOURCES_NAME = "sources";

  /** The name of the contract with the tables that the pipeline writes */
  public static final String SINKS_NAME = "sinks";

  private final DataContractLoader loader;
  private final TableContracts tableContracts;
  private final MainScript mainScript;
  private final ErrorCollector errors;

  /**
   * A table of a contract of the deployment.
   *
   * @param definition what identifies the table. The same statement that is planned more than once
   *     is one definition, and is described once unless its columns differ, as they can for a sink
   *     that takes the columns of the exported table
   * @param provided the entry that a contract file provides for the table
   */
  private record Described(
      Object definition, ContractSubject subject, Optional<AttachedContract> provided) {}

  /**
   * Compares the tables that the scripts define with the contracts that the project provides for
   * them, and reports the contract files that are not applied. All violations are recorded without
   * aborting, like tag violations, so that a compilation reports all of them together.
   *
   * <p>This only needs the planned tables, so it runs before the DAG is optimized.
   */
  public void check(Sqrl2FlinkSQLTranslator environment) {
    var definitions = environment.getTagAnalyzer().getDefinitions();
    // The same statement can be planned more than once, e.g. imported under two names
    tableContracts.getTables().stream()
        .filter(table -> table.contract().isPresent())
        .map(table -> described(table, environment))
        .distinct()
        .forEach(table -> compare(table, definitions));
    checkContractFiles(definitions);
  }

  /**
   * Assembles the contracts of the tables that the deployed pipeline reads, writes and publishes,
   * in every format.
   *
   * @param dag the optimized DAG, which only has the tables that are deployed or tested
   * @param plan the physical plan, which has the public tables as the engines store them
   * @return the content of each contract file, by its name
   */
  public Map<String, String> generate(
      PipelineDAG dag, PhysicalPlan plan, Sqrl2FlinkSQLTranslator environment) {

    var deployed = deployedNodes(dag);
    var deployedTables = StreamUtil.filterByClass(deployed, TableNode.class).toList();

    var sources =
        deployedTables.stream()
            .map(TableNode::getTableAnalysis)
            .filter(TableAnalysis::isSourceOrSink)
            .flatMap(table -> createdTable(table.getObjectIdentifier(), environment).stream())
            .toList();
    var sinks =
        StreamUtil.filterByClass(deployed, ExportNode.class)
            .flatMap(export -> exportedTable(export, dag, environment).stream())
            .toList();
    var publicTables =
        PublicTables.of(deployedTables, plan).stream()
            .map(table -> new Described(table, table, Optional.empty()))
            .toList();

    // The public tables share a contract with the source or sink tables if the main script has
    // the name of that contract. They are listed after those tables
    var groups = new TreeMap<String, List<List<Described>>>(String.CASE_INSENSITIVE_ORDER);
    groups.computeIfAbsent(SOURCES_NAME, name -> new ArrayList<>()).add(sources);
    groups.computeIfAbsent(SINKS_NAME, name -> new ArrayList<>()).add(sinks);
    groups.computeIfAbsent(mainScriptName(), name -> new ArrayList<>()).add(publicTables);

    var definitions = environment.getTagAnalyzer().getDefinitions();
    var documents = new LinkedHashMap<String, String>();
    for (var format : loader.getFormats()) {
      groups.forEach(
          (name, tables) -> {
            var entries =
                tables.stream()
                    .flatMap(group -> entries(group, format, definitions).stream())
                    .toList();
            if (entries.isEmpty()) {
              return;
            }
            if (loader.hasContractFiles()) {
              // Only a project that has contract files gets messages about data contracts
              warnSameNames(name, entries, format);
            }
            documents.put(format.fileName(name), format.write(name, entries));
          });
    }

    return documents;
  }

  /**
   * Compares a created table with its contract, whether or not the pipeline deploys the table, so
   * that a contract cannot drift unnoticed.
   */
  private static void compare(Described table, TagDefinitions definitions) {
    var contract = table.provided().orElseThrow();
    var file = contract.file();
    var differences = file.getFormat().compare(contract.entry(), table.subject(), definitions);
    if (differences.isEmpty()) {
      return;
    }

    differences.forEach(
        difference -> file.errorsAt(difference.location()).fatal("%s", difference.message()));

    // The error code, with its description, is given once per table
    file.errorsAt(contract.entry().location())
        .fatal(
            ErrorCode.INCOMPATIBLE_DATA_CONTRACT,
            "Table [%s] differs from its data contract in %s",
            table.subject().name(),
            differences.size() == 1 ? "one place" : differences.size() + " places");
  }

  /**
   * Reports the contract files and the entries that are not applied to a table:
   *
   * <ul>
   *   <li>an error for a file of which no entry matches a table, since the names are most likely
   *       wrong,
   *   <li>a notice for an entry without a table in a file that is applied, since a contract may
   *       describe more tables than its script defines,
   *   <li>a warning for a file that is skipped because this compilation does not plan the script
   *       next to it, such as the connectors of another environment.
   * </ul>
   */
  void checkContractFiles(TagDefinitions definitions) {
    for (var file : loader.getLoadedFiles()) {
      var name = file.getPath().getFileName();
      var label = file.getFormat().getEntryLabel();

      if (file.getAppliedTableNames().isEmpty()) {
        file.errorsAtEntries()
            .fatal(
                ErrorCode.INVALID_DATA_CONTRACT,
                "Contract file [%s] was not applied: none of its %ss %s matches a table",
                name,
                label,
                file.getUnappliedEntryNames());
      } else {
        file.getUnappliedEntries()
            .forEach(
                entry ->
                    file.errorsAt(entry.location())
                        .notice(
                            "%s [%s] matches no table and is not checked",
                            StringUtils.capitalize(label), entry.name()));
      }

      if (!definitions.isEnabled() && file.declaresTags()) {
        file.errorsAtEntries()
            .warn(
                "Contract file [%s] declares tags or a classification, which are not compared with"
                    + " the tables since compiler.tags is not configured",
                name);
      }
    }

    loader
        .getSkippedFiles()
        .forEach(
            file ->
                errors.warn(
                    "Contract file [%s] is skipped: the script next to it is not part of this"
                        + " compile",
                    file));
  }

  /**
   * The nodes that the deployed pipeline uses: everything that a sink reads, other than the sinks
   * that only tests query.
   */
  private static Set<PipelineNode> deployedNodes(PipelineDAG dag) {
    var sinks =
        dag.getSinks().stream()
            .filter(
                node ->
                    !(node instanceof TableFunctionNode function
                        && function.getFunction().getVisibility().isTest()))
            .toList();
    return dag.getAllInputsFromSource(sinks, true);
  }

  /** A table that a {@code CREATE TABLE} statement defines, unless it is a test table. */
  private Optional<Described> createdTable(
      ObjectIdentifier tableId, Sqrl2FlinkSQLTranslator environment) {
    return tableContracts
        .get(tableId)
        .filter(table -> !table.created().hintsAndDoc().hints().isTest())
        .map(table -> described(table, environment));
  }

  private static Described described(ContractTable table, Sqrl2FlinkSQLTranslator environment) {
    return new Described(
        table.definition(),
        ContractSubject.ofCreatedTable(table.created(), environment),
        table.contract());
  }

  private Optional<Described> exportedTable(
      ExportNode export, PipelineDAG dag, Sqrl2FlinkSQLTranslator environment) {

    var exported =
        StreamUtil.filterByClass(dag.getInputs(export), TableNode.class)
            .map(TableNode::getTableAnalysis)
            .findFirst();

    if (exported.isEmpty() || exported.get().getHints().isTest()) {
      return Optional.empty();
    }

    if (export.getCreatedSinkTable().isPresent()) {
      return createdTable(export.getCreatedSinkTable().get(), environment);
    }

    if (isLogExport(export)) {
      // The log engine creates the topic for the exported table
      var table = exported.get();
      var topic =
          ContractSubject.ofComputedTable(
                  export.getSinkPath().getLast().getDisplay(), table.getRowType(), table)
              .storageType(Optional.of(StorageType.TOPIC))
              .build();
      return Optional.of(new Described(export.getSinkPath(), topic, Optional.empty()));
    }

    return Optional.empty();
  }

  /** {@code EXPORT … TO log.X}, unlike the print and logger sinks, writes a topic. */
  private static boolean isLogExport(ExportNode export) {
    return export.getSinkTo().isPresent()
        && export.getSinkPath().size() == 2
        && SystemBuiltInConnectors.forExport(export.getSinkPath().getFirst())
            .filter(connector -> connector == SystemBuiltInConnectors.LOG_ENGINE)
            .isPresent();
  }

  /** The name of the main script without its extension. */
  private String mainScriptName() {
    return mainScript
        .getPath()
        .map(path -> path.getFileName().toString())
        .flatMap(DataContractLoader::stemOf)
        .orElse("main");
  }

  /**
   * The entries of a contract in the given format, without the duplicates of the same definition,
   * and sorted by name, so that the output is deterministic.
   */
  private static List<GeneratedEntry> entries(
      List<Described> tables, DataContractFormat format, TagDefinitions definitions) {
    record Rendered(Object definition, GeneratedEntry entry) {}
    var rendered = new LinkedHashSet<Rendered>();
    for (var table : tables) {
      var provided =
          table
              .provided()
              .filter(contract -> contract.file().getFormat() == format)
              .map(AttachedContract::entry);
      rendered.add(
          new Rendered(
              table.definition(), format.describe(table.subject(), provided, definitions)));
    }
    return rendered.stream()
        .map(Rendered::entry)
        .sorted(
            Comparator.comparing(GeneratedEntry::name, String.CASE_INSENSITIVE_ORDER)
                .thenComparing(GeneratedEntry::name)
                .thenComparing(GeneratedEntry::toString))
        .toList();
  }

  private void warnSameNames(String name, List<GeneratedEntry> entries, DataContractFormat format) {
    var label = format.getEntryLabel();
    var names = new HashSet<String>();
    entries.stream()
        .map(GeneratedEntry::name)
        .filter(entryName -> !names.add(entryName.toLowerCase(Locale.ROOT)))
        .distinct()
        .sorted()
        .forEach(
            entryName ->
                errors.warn(
                    "The data contract [%s] has more than one %s named [%s], since more than"
                        + " one table of the pipeline has that name. A consumer that finds a %s"
                        + " by its name cannot tell them apart",
                    format.fileName(name), label, entryName, label));
  }
}
