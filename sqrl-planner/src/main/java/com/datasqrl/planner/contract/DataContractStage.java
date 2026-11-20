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
import com.datasqrl.planner.contract.DataContracts.ContractFileUsage;
import com.datasqrl.planner.contract.DataContracts.Document;
import com.datasqrl.planner.contract.OdcsCompatibilityChecker.Difference;
import com.datasqrl.planner.contract.TableContract.AttachedContract;
import com.datasqrl.planner.contract.model.OdcsSchemaObject;
import com.datasqrl.planner.dag.PipelineDAG;
import com.datasqrl.planner.dag.nodes.ExportNode;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.datasqrl.planner.dag.nodes.TableFunctionNode;
import com.datasqrl.planner.dag.nodes.TableNode;
import com.datasqrl.planner.tables.StorageType;
import com.datasqrl.planner.tags.TagDefinitions;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Checks the data contracts that the project provides against its tables, and assembles the data
 * contracts of the deployment:
 *
 * <ul>
 *   <li>{@code sources} and {@code sinks} describe the tables that the deployed pipeline reads and
 *       writes. A table has the schema object that a contract file provides for it, and a generated
 *       one otherwise.
 *   <li>The document named after the main script describes the public tables, see {@link
 *       PublicTables}. They are always generated.
 * </ul>
 *
 * A contract file next to a planned script is either applied or reported, so that a contract that
 * the compiler does not pick up is not mistaken for one that passed.
 */
@Component
@Lazy
@RequiredArgsConstructor
public class DataContractStage {

  private final DataContractLoader loader;
  private final WorkspacePaths workspacePaths;
  private final MainScript mainScript;
  private final ErrorCollector errors;

  /**
   * Compares the tables that the scripts define with the contracts that the project provides for
   * them, and reports the contract files that are not applied. All violations are recorded without
   * aborting, like tag violations, so that a compilation reports all of them together.
   *
   * <p>This only needs the planned tables, so it runs before the DAG is optimized.
   *
   * @return how the contract files of the project were used
   */
  public List<ContractFileUsage> check(Sqrl2FlinkSQLTranslator environment) {
    var collector = errors.abortOnFatal(false);
    var definitions = environment.getTagAnalyzer().getDefinitions();
    checkProvidedContracts(environment, definitions, collector);
    return checkContractFiles(definitions, collector);
  }

  /**
   * Assembles the contracts of the tables that the deployed pipeline reads, writes and publishes.
   *
   * @param dag the optimized DAG, which only has the tables that are deployed or tested
   * @param plan the physical plan, which has the public tables as the engines store them
   * @param files the result of {@link #check}
   */
  public DataContracts generate(
      PipelineDAG dag,
      PhysicalPlan plan,
      Sqrl2FlinkSQLTranslator environment,
      List<ContractFileUsage> files) {
    var generator = new OdcsGenerator(environment.getTagAnalyzer().getDefinitions());
    var deployed = deployedNodes(dag);
    var deployedTables =
        deployed.stream().filter(TableNode.class::isInstance).map(TableNode.class::cast).toList();

    var sources = new SchemaObjects();
    deployedTables.stream()
        .map(TableNode::getTableAnalysis)
        .filter(TableAnalysis::isSourceOrSink)
        .forEach(table -> addCreatedTable(sources, table, generator, environment));
    var sinks = new SchemaObjects();
    deployed.stream()
        .filter(ExportNode.class::isInstance)
        .map(ExportNode.class::cast)
        .forEach(export -> addExport(sinks, export, dag, generator, environment));
    var publicTables = new SchemaObjects();
    PublicTables.of(deployedTables, plan)
        .forEach(
            table ->
                publicTables.add(
                    table, OdcsYaml.toJson(generator.generate(table, OdcsVersion.LATEST))));

    // The public tables share a document with the source or sink tables if the main script has
    // the name of that document
    var schemaObjects = new TreeMap<String, List<JsonNode>>(String.CASE_INSENSITIVE_ORDER);
    schemaObjects.put(OdcsConstants.SOURCES_NAME, sources.sorted());
    schemaObjects.put(OdcsConstants.SINKS_NAME, sinks.sorted());
    schemaObjects
        .computeIfAbsent(mainScriptName(), name -> new ArrayList<>())
        .addAll(publicTables.sorted());

    var documents =
        schemaObjects.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty())
            .map(entry -> new Document(entry.getKey(), List.copyOf(entry.getValue())))
            .toList();
    if (!files.isEmpty()) {
      // Only a project that has contract files gets messages about data contracts
      documents.forEach(this::warnSameNames);
    }
    return new DataContracts(documents, files);
  }

  /**
   * Compares every created table that has a contract with its contract, whether or not the pipeline
   * deploys the table, so that a contract cannot drift unnoticed.
   */
  private static void checkProvidedContracts(
      Sqrl2FlinkSQLTranslator environment, TagDefinitions definitions, ErrorCollector collector) {
    var generator = new OdcsGenerator(definitions);
    var checker = new OdcsCompatibilityChecker(definitions);
    // The same statement can be planned more than once, e.g. imported under two names
    var checked = new HashSet<CheckedTable>();
    for (var table : environment.getTableLookup().getSourceTables()) {
      var contract = table.getSourceSinkTable().orElseThrow().definition().contract();
      if (contract.attached().isEmpty()) {
        continue;
      }
      var attached = contract.attached().get();
      var subject = ContractSubject.ofCreatedTable(table, environment.getTypeFactory());
      var generated = generator.generate(subject, attached.file().getVersion());
      if (checked.add(new CheckedTable(contract, generated))) {
        var differences =
            checker.compare(
                OdcsYaml.toSchemaObject(attached.schemaObject()),
                attached.pointer(),
                generated,
                subject);
        report(differences, attached, subject, collector);
      }
    }
  }

  /** A table definition with the schema object generated for it, to check each one once */
  private record CheckedTable(TableContract contract, OdcsSchemaObject generated) {}

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
                .errorsAt(collector, difference.pointer())
                .fatal("%s", difference.message()));
    // The error code, with its description, is given once per table
    contract
        .file()
        .errorsAt(collector, contract.pointer())
        .fatal(
            ErrorCode.INCOMPATIBLE_DATA_CONTRACT,
            "Table [%s] differs from its data contract in %s",
            subject.name(),
            differences.size() == 1 ? "one place" : differences.size() + " places");
  }

  /**
   * Reports the loaded contract files that were not applied, and collects how the contract files of
   * the project were used.
   */
  List<ContractFileUsage> checkContractFiles(TagDefinitions definitions, ErrorCollector collector) {
    var usages = new ArrayList<ContractFileUsage>();
    for (var file : loader.getLoadedFiles()) {
      var name = file.getPath().getFileName();
      if (file.getAppliedTableNames().isEmpty()) {
        file.errorsAt(collector, "/" + OdcsConstants.SCHEMA_FIELD)
            .fatal(
                ErrorCode.INVALID_DATA_CONTRACT,
                "Contract file [%s] was not applied: none of its schema objects %s matches a table",
                name,
                file.getUnappliedObjectNames());
      }
      if (!definitions.isEnabled() && file.declaresTags()) {
        file.errorsAt(collector, "/" + OdcsConstants.SCHEMA_FIELD)
            .warn(
                "Contract file [%s] declares tags or a classification, which are not compared with"
                    + " the tables since compiler.tags is not configured",
                name);
      }
      usages.add(
          new ContractFileUsage(
              file.getPath(), true, file.getAppliedTableNames(), file.getUnappliedObjectNames()));
    }

    // The other contract files of the project sit next to scripts that this compilation does not
    // plan, such as the connectors of another environment
    var found = loader.getFoundFiles();
    contractFilesInBuildDir().stream()
        .filter(file -> !found.contains(file))
        .forEach(file -> usages.add(new ContractFileUsage(file, false, List.of(), List.of())));

    usages.sort(Comparator.comparing(ContractFileUsage::file));
    return List.copyOf(usages);
  }

  /** The contract files of the project and its includes, which preprocessing copied. */
  private List<Path> contractFilesInBuildDir() {
    var buildDir = workspacePaths.buildDir().toAbsolutePath().normalize();
    if (!Files.isDirectory(buildDir)) {
      return List.of();
    }
    try (var files = Files.walk(buildDir)) {
      return files
          .filter(file -> file.getFileName().toString().endsWith(OdcsConstants.FILE_SUFFIX))
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

  private static void addCreatedTable(
      SchemaObjects schemaObjects,
      TableAnalysis table,
      OdcsGenerator generator,
      Sqrl2FlinkSQLTranslator environment) {
    var definition = table.getSourceSinkTable().orElseThrow().definition();
    if (definition.isTest()) {
      return;
    }
    var subject = ContractSubject.ofCreatedTable(table, environment.getTypeFactory());
    var generated = generator.generate(subject, OdcsVersion.LATEST);
    // The documents have the latest ODCS version. A schema object of an older contract is taken
    // as it is if the table has no type that the versions describe differently
    var schemaObject =
        definition
            .contract()
            .attached()
            .filter(
                attached ->
                    generated.equals(generator.generate(subject, attached.file().getVersion())))
            .map(AttachedContract::schemaObject)
            .orElseGet(() -> OdcsYaml.toJson(generated));
    schemaObjects.add(definition.contract(), schemaObject);
  }

  private static void addExport(
      SchemaObjects schemaObjects,
      ExportNode export,
      PipelineDAG dag,
      OdcsGenerator generator,
      Sqrl2FlinkSQLTranslator environment) {
    var exported =
        dag.getInputs(export).stream()
            .filter(TableNode.class::isInstance)
            .map(input -> ((TableNode) input).getTableAnalysis())
            .findFirst();
    if (exported.isEmpty() || exported.get().getHints().isTest()) {
      return;
    }

    if (export.getCreatedSinkTable().isPresent()) {
      var sinkTable =
          environment.getTableLookup().lookupSourceTable(export.getCreatedSinkTable().get());
      addCreatedTable(schemaObjects, sinkTable, generator, environment);
    } else if (isLogExport(export)) {
      // The log engine creates the topic for the exported table
      var table = exported.get();
      var topic =
          ContractSubject.ofComputedTable(
                  export.getSinkPath().getLast().getDisplay(), table.getRowType(), table)
              .storageType(Optional.of(StorageType.TOPIC))
              .build();
      schemaObjects.add(
          export.getSinkPath(), OdcsYaml.toJson(generator.generate(topic, OdcsVersion.LATEST)));
    }
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

  private void warnSameNames(Document document) {
    var names = new HashSet<String>();
    document.schema().stream()
        .map(schemaObject -> schemaObject.path(OdcsConstants.NAME_FIELD).asText(""))
        .filter(name -> !names.add(DataContractFile.key(name)))
        .distinct()
        .sorted()
        .forEach(
            name ->
                errors.warn(
                    "The data contract [%s] has more than one schema object named [%s], since"
                        + " more than one table of the pipeline has that name. A consumer that"
                        + " finds a schema object by its name cannot tell them apart",
                    document.fileName(), name));
  }

  /** The schema objects of a document, without the duplicates of the same definition. */
  private static final class SchemaObjects {

    /**
     * @param definition what identifies the table. The same statement that is planned more than
     *     once is one definition, and has one schema object unless its columns differ, as they can
     *     for a sink that takes the columns of the exported table
     */
    private record Entry(Object definition, JsonNode schemaObject) {

      String name() {
        return schemaObject.path(OdcsConstants.NAME_FIELD).asText("");
      }
    }

    private final Collection<Entry> entries = new LinkedHashSet<>();

    void add(Object definition, JsonNode schemaObject) {
      entries.add(new Entry(definition, schemaObject));
    }

    /** The schema objects sorted by name, so that the output is deterministic. */
    List<JsonNode> sorted() {
      return entries.stream()
          .sorted(
              Comparator.comparing(Entry::name, String.CASE_INSENSITIVE_ORDER)
                  .thenComparing(Entry::name)
                  .thenComparing(entry -> entry.schemaObject().toString()))
          .map(Entry::schemaObject)
          .collect(Collectors.toCollection(ArrayList::new));
    }
  }
}
