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
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.MainScript;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.contract.DataContractFormat.Difference;
import com.datasqrl.planner.contract.DataContractFormat.GeneratedEntry;
import com.datasqrl.planner.contract.DataContracts.Document;
import com.datasqrl.planner.contract.TableContracts.AttachedContract;
import com.datasqrl.planner.contract.TableContracts.ContractTable;
import com.datasqrl.planner.dag.PipelineDAG;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.tables.SourceSinkTableAnalysis;
import com.datasqrl.planner.tables.StorageType;
import com.datasqrl.planner.tags.TagDefinitions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
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
  private final WorkspacePaths workspacePaths;
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
    var collector = errors.abortOnFatal(false);
    var definitions = environment.getTagAnalyzer().getDefinitions();
    checkProvidedContracts(environment, definitions, collector);
    checkContractFiles(definitions, collector);
  }

  /**
   * Assembles the contracts of the tables that the deployed pipeline reads, writes and publishes,
   * in every format.
   *
   * @param dag the optimized DAG, which only has the tables that are deployed or tested
   * @param plan the physical plan, which has the public tables as the engines store them
   */
  public DataContracts generate(
      PipelineDAG dag, PhysicalPlan plan, Sqrl2FlinkSQLTranslator environment) {
    var deployed = deployedNodes(dag);
    var deployedTables =
        deployed.stream().filter(TableNode.class::isInstance).map(TableNode.class::cast).toList();

    var sources =
        deployedTables.stream()
            .map(TableNode::getTableAnalysis)
            .filter(TableAnalysis::isSourceOrSink)
            .flatMap(table -> createdTable(table.getObjectIdentifier(), environment).stream())
            .toList();
    var sinks =
        deployed.stream()
            .filter(ExportNode.class::isInstance)
            .map(ExportNode.class::cast)
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
    // Only a project that has contract files gets messages about data contracts
    var hasContractFiles =
        !loader.getFoundFiles().isEmpty() || !contractFilesInBuildDir().isEmpty();
    var documents = new ArrayList<Document>();
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
            if (hasContractFiles) {
              warnSameNames(name, entries, format);
            }
            documents.add(new Document(format.fileName(name), format.write(name, entries)));
          });
    }
    return new DataContracts(List.copyOf(documents));
  }

  /**
   * Compares every created table that has a contract with its contract, whether or not the pipeline
   * deploys the table, so that a contract cannot drift unnoticed.
   */
  private void checkProvidedContracts(
      Sqrl2FlinkSQLTranslator environment, TagDefinitions definitions, ErrorCollector collector) {
    // The same statement can be planned more than once, e.g. imported under two names
    var checked = new HashSet<Described>();
    for (var table : tableContracts.getTables()) {
      if (table.contract().isEmpty()) {
        continue;
      }
      var attached = table.contract().get();
      var subject = subject(table, environment);
      if (checked.add(new Described(table.definition(), subject, table.contract()))) {
        var differences =
            attached.file().getFormat().compare(attached.entry(), subject, definitions);
        report(differences, attached, subject, collector);
      }
    }
  }

  private static void report(
      List<Difference> differences,
      AttachedContract contract,
      ContractSubject subject,
      ErrorCollector collector) {
    if (differences.isEmpty()) {
      return;
    }
    differences.forEach(
        difference ->
            contract
                .file()
                .errorsAt(collector, difference.location())
                .fatal("%s", difference.message()));
    // The error code, with its description, is given once per table
    contract
        .file()
        .errorsAt(collector, contract.entry().location())
        .fatal(
            ErrorCode.INCOMPATIBLE_DATA_CONTRACT,
            "Table [%s] differs from its data contract in %s",
            subject.name(),
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
  void checkContractFiles(TagDefinitions definitions, ErrorCollector collector) {
    for (var file : loader.getLoadedFiles()) {
      var name = file.getPath().getFileName();
      var label = file.getFormat().getEntryLabel();

      if (file.getAppliedTableNames().isEmpty()) {
        file.errorsAtEntries(collector)
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
                    file.errorsAt(collector, entry.location())
                        .notice(
                            "%s [%s] matches no table and is not checked",
                            StringUtils.capitalize(label), entry.name()));
      }

      if (!definitions.isEnabled() && file.declaresTags()) {
        file.errorsAtEntries(collector)
            .warn(
                "Contract file [%s] declares tags or a classification, which are not compared with"
                    + " the tables since compiler.tags is not configured",
                name);
      }
    }

    // The other contract files of the project sit next to scripts that this compilation does not
    // plan. They are reported in the order of their paths, whatever order the folder is walked in
    var buildDir = workspacePaths.buildDir().toAbsolutePath().normalize();
    var found = loader.getFoundFiles();
    contractFilesInBuildDir().stream()
        .filter(file -> !found.contains(file))
        .map(buildDir::relativize)
        .sorted()
        .forEach(
            file ->
                collector.warn(
                    "Contract file [%s] is skipped: the script next to it is not part of this"
                        + " compile",
                    file));
  }

  /** The contract files of the project and its includes, which preprocessing copied. */
  private List<Path> contractFilesInBuildDir() {
    var buildDir = workspacePaths.buildDir().toAbsolutePath().normalize();
    if (!Files.isDirectory(buildDir)) {
      return List.of();
    }
    var suffixes =
        loader.getFormats().stream().map(format -> "." + format.getFileExtension()).toList();
    try (var files = Files.walk(buildDir)) {
      return files
          .filter(file -> suffixes.stream().anyMatch(file.getFileName().toString()::endsWith))
          .filter(Files::isRegularFile)
          .toList();
    } catch (IOException e) {
      throw new IllegalStateException("Could not list the files in " + buildDir, e);
    }
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
        .filter(table -> !table.isTest())
        .map(
            table ->
                new Described(table.definition(), subject(table, environment), table.contract()));
  }

  private Optional<Described> exportedTable(
      ExportNode export, PipelineDAG dag, Sqrl2FlinkSQLTranslator environment) {
    var exported =
        dag.getInputs(export).stream()
            .filter(TableNode.class::isInstance)
            .map(input -> ((TableNode) input).getTableAnalysis())
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

  private static ContractSubject subject(ContractTable table, Sqrl2FlinkSQLTranslator environment) {
    return ContractSubject.ofCreatedTable(
        table, partitionKeys(table, environment), environment.getTypeFactory());
  }

  /**
   * The partition columns as the statement declares them. An engine may remove them from the table
   * and keep them in the mutation, e.g. Kafka uses them as the message key.
   */
  private static List<String> partitionKeys(
      ContractTable table, Sqrl2FlinkSQLTranslator environment) {
    var mutation =
        table.table().getSourceSinkTable().map(SourceSinkTableAnalysis::mutationDefinition);
    if (mutation.isPresent() && !mutation.get().getPartitionKey().isEmpty()) {
      return mutation.get().getPartitionKey();
    }
    return environment
        .getCatalogManager()
        .getTable(table.id())
        .map(resolved -> resolved.<ResolvedCatalogTable>getResolvedTable().getPartitionKeys())
        .orElse(List.of());
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
