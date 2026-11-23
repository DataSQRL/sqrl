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
package com.datasqrl;

import static com.datasqrl.SnapshotTestSupport.getResourcesDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.planner.contract.odcs.OdcsConstants;
import com.datasqrl.planner.contract.odcs.OdcsYaml;
import com.datasqrl.util.JsonSchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Checks the data contracts in the deployment folder beyond what the snapshots of the {@code
 * contracts-*} use cases show.
 */
class DataContractDeploymentTest {

  private static final Path USE_CASE = getResourcesDirectory("usecases/contracts-compile");
  private static final Path ENVIRONMENTS = getResourcesDirectory("usecases/contracts-environments");

  private static final String PACKAGE =
      """
      {
        "version": "1",
        "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
        "script": { "main": "main.sqrl" }
      }
      """;

  private static final String ITEMS =
      """
      /** Items of the catalog */
      CREATE TABLE Items (
        id BIGINT NOT NULL,
        title VARCHAR(200) NOT NULL,
        price DECIMAL(10, 2),
        attributes MAP<STRING, STRING>,
        changed_at TIMESTAMP_LTZ(3) NOT NULL,
        PRIMARY KEY (id) NOT ENFORCED,
        WATERMARK FOR changed_at AS changed_at - INTERVAL '1' SECOND
      ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///tmp/contracts-test/items',
        'format' = 'flexible-json'
      );
      """;

  @RegisterExtension final CliCompileTestExtension compiler = new CliCompileTestExtension();

  @TempDir Path project;

  @Test
  void givenContractsUseCase_whenCompiled_thenAllFilesValidateAgainstOdcsSchema() throws Exception {
    var hook = compile(USE_CASE, "compile", "package.json");

    // Scripts without a contract file, and a contract file of a script that is not imported,
    // cause no message
    assertThat(hook.getMessages()).doesNotContain("[WARN]", "[FATAL]", "odcs.yaml");

    try (var files = Files.list(compiler.getContractsDir())) {
      var contracts = files.sorted().toList();

      assertThat(contracts)
          .extracting(path -> path.getFileName().toString())
          .containsExactly("contracts.odcs.yaml", "sinks.odcs.yaml", "sources.odcs.yaml");
      for (var contract : contracts) {
        assertThat(
                JsonSchemaValidator.validate(
                    readContract(contract), OdcsConstants.JSON_SCHEMA_RESOURCE))
            .as(contract.getFileName().toString())
            .isEmpty();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"compile", "test"})
  void givenUnusedAndTestOnlyTables_whenCompiled_thenTheyHaveNoContract(String goal) {
    compile(USE_CASE, goal, "package.json");

    assertThat(schemaObjectNames(contract("sources")))
        .containsExactly(
            "Clicks", "CUSTOMERS", "Events", "Feedback", "LargeOrders", "Orders", "Products");
    assertThat(schemaObjectNames(contract("sinks")))
        .containsExactly("EmailTopic", "EventKinds", "LargeOrders");
    assertThat(schemaObjectNames(contract("contracts"))).doesNotContain("AuditTest");
  }

  @Test
  void givenEnvironmentFiles_whenCompiledForEachEnvironment_thenBothUseTheBaseFileContract() {
    compile(ENVIRONMENTS, "compile", "package-test.json");
    var test = contract("sources");
    compile(ENVIRONMENTS, "compile", "package-prod.json");
    var prod = contract("sources");

    assertThat(schemaObjectNames(test)).containsExactly("Customers", "Orders");
    assertThat(prod).isEqualTo(test);
  }

  @Test
  void givenProjectWithoutContractFiles_whenCompiled_thenNoGuardRuns() {
    write("package.json", PACKAGE);
    write(
        "main.sqrl",
        ITEMS + "/** Titles of the items */\nItemTitles := SELECT id, title FROM Items;\n");

    var hook = compile(project, "compile", "package.json");

    assertThat(hook.getMessages()).doesNotContainIgnoringCase("contract");
    assertThat(schemaObjectNames(contract("sources"))).containsExactly("Items");
    assertThat(compiler.getContractsDir().resolve("sinks.odcs.yaml")).doesNotExist();
    assertThat(compiler.getContractsDir().resolve("main.odcs.yaml")).doesNotExist();
  }

  @Test
  void givenGeneratedContracts_whenCopiedIntoDefiningFile_thenRecompileSucceedsWithSameContracts()
      throws Exception {
    write("package.json", PACKAGE);
    write(
        "main.sqrl",
        ITEMS
            + """
              /** Archive of the items */
              CREATE TABLE ItemArchive (
                id BIGINT NOT NULL,
                title STRING NOT NULL
              ) WITH (
                'connector' = 'filesystem',
                'path' = 'file:///tmp/contracts-test/item-archive',
                'format' = 'flexible-json'
              );

              INSERT INTO ItemArchive SELECT id, title FROM Items;

              /** Titles of the items */
              ItemTitles := SELECT id, title FROM Items;
              """);
    compile(project, "compile", "package.json");
    var sources = readFile("sources");
    var sinks = readFile("sinks");

    // The schema objects of the generated contracts become the contract of the script
    var schemaObjects = new LinkedHashMap<String, JsonNode>();
    for (var generated : List.of(contract("sources"), contract("sinks"))) {
      generated
          .path("schema")
          .forEach(
              schemaObject ->
                  schemaObjects.putIfAbsent(schemaObject.path("name").asText(), schemaObject));
    }
    var contract = JsonNodeFactory.instance.objectNode();
    contract.put("apiVersion", "v3.2.0");
    contract.put("kind", "DataContract");
    contract.put("id", "main");
    contract.put("version", "1.0.0");
    contract.put("status", "active");
    contract.putArray("schema").addAll(schemaObjects.values());
    write("main.odcs.yaml", OdcsYaml.write(contract));

    var hook = compile(project, "compile", "package.json");

    assertThat(schemaObjects).containsOnlyKeys("Items", "ItemArchive");
    assertThat(hook.getMessages()).doesNotContainIgnoringCase("contract");
    assertThat(readFile("sources")).isEqualTo(sources);
    assertThat(readFile("sinks")).isEqualTo(sinks);
  }

  @Test
  void givenMainScriptNamedSources_whenCompiled_thenPublicTablesAreWrittenIntoTheSourcesFile()
      throws Exception {
    write(
        "package.json",
        """
        {
          "version": "1",
          "enabled-engines": ["vertx", "flink", "iceberg", "duckdb"],
          "script": { "main": "sources.sqrl" },
          "connectors": { "iceberg": { "warehouse": "/tmp/contracts-test/warehouse" } }
        }
        """);
    write(
        "sources.sqrl",
        ITEMS
            + """
              /*+exec(iceberg) */
              /** Titles of the items */
              ItemTitles := SELECT id, title FROM Items;
              """);
    write(
        "sources.odcs.yaml",
        """
        apiVersion: v3.2.0
        kind: DataContract
        id: catalog
        version: 1.0.0
        status: active
        schema:
          - name: Items
            physicalType: catalog-table
            properties:
              - name: id
                logicalType: integer
                required: true
              - name: title
                logicalType: string
                required: true
              - name: price
                logicalType: number
              - name: attributes
                logicalType: map
                map:
                  key:
                    logicalType: string
                  value:
                    logicalType: string
              - name: changed_at
                logicalType: timestamp
                required: true
        """);

    var hook = compile(project, "compile", "package.json");

    try (var files = Files.list(compiler.getContractsDir())) {
      assertThat(files)
          .extracting(path -> path.getFileName().toString())
          .containsExactly("sources.odcs.yaml");
    }
    // The source table with its provided schema object, then the public tables: the table that
    // the pipeline writes and the view
    assertThat(contract("sources").path("schema"))
        .extracting(
            schemaObject -> schemaObject.path("name").asText(),
            schemaObject -> schemaObject.path("physicalType").asText())
        .containsExactly(
            tuple("Items", "catalog-table"), tuple("Items", "table"), tuple("ItemTitles", "view"));
    assertThat(hook.getMessages())
        .contains(
            "The data contract [sources.odcs.yaml] has more than one schema object named [Items]");
    assertThat(hook.getMessages()).doesNotContain("[FATAL]");
  }

  @Test
  void givenContractsOfAnEarlierCompileInAnotherTargetFolder_whenCompiled_thenTheyAreIgnored() {
    write("package.json", PACKAGE);
    write(
        "main.sqrl",
        ITEMS + "/** Titles of the items */\nItemTitles := SELECT id, title FROM Items;\n");
    compile(project, "compile", "package.json", "-t", "artifacts");
    assertThat(project.resolve("artifacts/contracts/sources.odcs.yaml")).isRegularFile();

    var hook = compile(project, "compile", "package.json");

    assertThat(hook.getMessages()).doesNotContainIgnoringCase("contract");
    assertThat(schemaObjectNames(contract("sources"))).containsExactly("Items");
  }

  @Test
  void givenStandaloneInclude_whenCompiled_thenSinksOfItsOwnDeploymentHaveNoContract() {
    // The project imports a script of the include that exports to the log and to two tables
    var useCase = getResourcesDirectory("usecases/kafka-include-subscribe-compile");

    compile(useCase, "compile", "package.json");
    var inline = sinkNames();
    // The deployment of a standalone include runs the exports of its scripts itself
    compile(useCase, "compile", "package-standalone.json");
    var standalone = sinkNames();

    assertThat(inline).contains("OrderArchive", "OrderAudit", "SmallOrderLog");
    assertThat(standalone).doesNotContain("OrderArchive", "OrderAudit", "SmallOrderLog");
  }

  /** The names of the sink tables, which have no contract file if there are none. */
  private List<String> sinkNames() {
    return Files.exists(compiler.getContractsDir().resolve(OdcsConstants.fileName("sinks")))
        ? schemaObjectNames(contract("sinks"))
        : List.of();
  }

  @Test
  void givenIncludedProject_whenCompiled_thenContractIsResolvedFromIncludeBuildDir() {
    write("shared/package.json", "{ \"version\": \"1\" }\n");
    write(
        "shared/catalog.sqrl",
        """
        CREATE TABLE Parts (
          id BIGINT NOT NULL,
          label STRING
        ) WITH (
          'connector' = 'datagen'
        );
        """);
    write(
        "shared/catalog.odcs.yaml",
        """
        apiVersion: v3.2.0
        kind: DataContract
        id: catalog
        version: 1.0.0
        status: active
        schema:
          - name: Parts
            description: From the contract of the included project
            properties:
              - name: id
                logicalType: integer
                required: true
              - name: label
                logicalType: string
        """);
    write(
        "app/package.json",
        """
        {
          "version": "1",
          "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
          "script": {
            "main": "main.sqrl",
            "include": { "shared": { "package": "../shared/package.json" } }
          }
        }
        """);
    write(
        "app/main.sqrl",
        """
        IMPORT shared.catalog.*;

        /** Labels of the parts */
        PartLabels := SELECT id, label FROM Parts;
        """);

    var hook = compile(project.resolve("app"), "compile", "package.json");

    assertThat(hook.getMessages()).doesNotContainIgnoringCase("contract");
    assertThat(
            schemaObject(contract("sources"), "Parts").orElseThrow().path("description").asText())
        .isEqualTo("From the contract of the included project");
  }

  private AssertStatusHook compile(Path workspace, String... args) {
    var hook = compiler.execute(workspace, args);
    assertThat(hook.isFailed()).as(hook.getMessages()).isFalse();
    return hook;
  }

  private JsonNode contract(String name) {
    return readContract(compiler.getContractsDir().resolve(OdcsConstants.fileName(name)));
  }

  @SneakyThrows
  private String readFile(String name) {
    return Files.readString(compiler.getContractsDir().resolve(OdcsConstants.fileName(name)));
  }

  @SneakyThrows
  private void write(String file, String content) {
    var path = project.resolve(file);
    Files.createDirectories(path.getParent());
    Files.writeString(path, content);
  }

  @SneakyThrows
  private static JsonNode readContract(Path file) {
    var documents = OdcsYaml.readDocuments(Files.readString(file));
    assertThat(documents).hasSize(1);
    return documents.get(0);
  }

  /** The names of the schema objects of a contract, in the order of the file. */
  private static List<String> schemaObjectNames(JsonNode contract) {
    return StreamSupport.stream(contract.path("schema").spliterator(), false)
        .map(schemaObject -> schemaObject.path("name").asText())
        .toList();
  }

  private static Optional<JsonNode> schemaObject(JsonNode contract, String name) {
    return StreamSupport.stream(contract.path("schema").spliterator(), false)
        .filter(schemaObject -> schemaObject.path("name").asText().equals(name))
        .findFirst();
  }
}
