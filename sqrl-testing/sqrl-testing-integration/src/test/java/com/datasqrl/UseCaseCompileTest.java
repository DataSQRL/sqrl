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

import com.datasqrl.deployment.model.JdbcPlanModel;
import com.datasqrl.deployment.model.JdbcStatementModel;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.util.ArgumentsProviders;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

/**
 * Parametrized Test for parsing and planning of SQRL scripts in resources/usecases. Add entire SQRL
 * projects to this folder to test the parser and planner. This test executes quickly.
 *
 * <p>This test snapshots the produced DAG plan and deployment assets. SQRL scripts with names
 * ending in `-fail` are expected to produce errors which are snapshotted. SQRL scripts ending in
 * `-warn` are expected to produce warnings which are snapshotted. SQRL scripts ending in -disabled`
 * are ignored.
 */
@ExtendWith(SystemStubsExtension.class)
public class UseCaseCompileTest {

  private static final Path USECASE_DIR = getResourcesDirectory("usecases");

  @SuppressWarnings("unused")
  @SystemStub
  private final EnvironmentVariables environmentVariables =
      new EnvironmentVariables(
          "DATAGEN_CONNECTOR", "datagen", "ICEBERG_TEST_WAREHOUSE", "/tmp/test_iceberg_wh");

  @RegisterExtension
  final CliCompileTestExtension snapshotExtension = new CliCompileTestExtension();

  @ParameterizedTest
  @ArgumentsSource(UseCaseFiles.class)
  void testUseCase(Path packageFile) {
    UseCaseTestHelper.testUseCase(
        snapshotExtension,
        getClass(),
        packageFile,
        UseCaseTestHelper.defaultBuildDirFilter(),
        UseCaseTestHelper.defaultPlanDirFilter());
  }

  @Test
  void givenImportedInserts_whenExecutedAfterDatabaseRestore_thenWriteOriginalDataToOriginalSinks(
      @TempDir Path output) throws Exception {
    var sql =
        compileInsertIdentityUseCase()
            .replace("file:///tmp/insert-table-identity", output.toUri().toString());
    var env = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inBatchMode());
    env.getConfig().getConfiguration().setString("parallelism.default", "1");
    try {
      var statements =
          ((PlannerBase) env.getPlanner()).createFlinkPlanner().parser().parseSqlList(sql);
      for (var statement : statements) {
        env.executeSql(RelToFlinkSql.convertToString(statement)).await(60, TimeUnit.SECONDS);
      }

      assertThat(readSinkRows(output.resolve("source-sink"))).containsExactly("11");
      assertThat(readSinkRows(output.resolve("target-sink"))).containsExactly("11");
      assertThat(readSinkRows(output.resolve("scope-sink"))).containsExactly("11");
      assertThat(readSinkRows(output.resolve("wrong-target"))).isEmpty();
    } finally {
      env.getCatalogManager().close();
    }
  }

  private static List<String> readSinkRows(Path directory) throws IOException {
    var rows = new ArrayList<String>();
    if (Files.exists(directory)) {
      try (var files = Files.walk(directory)) {
        for (var file :
            files.filter(p -> p.getFileName().toString().startsWith("part-")).toList()) {
          rows.addAll(Files.readAllLines(file));
        }
      }
    }
    return rows;
  }

  /**
   * Compiles a standalone include on its own, as its deployment does, and a project that includes
   * it, and checks that each Kafka topic has one writer: the project only reads the topics that the
   * include writes, and it writes none of them itself.
   */
  @ParameterizedTest
  @CsvSource({
    "kafka-include-compile, package-standalone.json",
    "kafka-include-subscribe-compile, package-standalone.json",
    "kafka-include-subscribe-compile, package-standalone-namespaced.json",
    "kafka-include-subscribe-compile, package-standalone-extras.json"
  })
  void givenStandaloneInclude_whenProjectAndIncludeAreCompiled_thenEachTopicHasOneWriter(
      String project, String packageFile) throws IOException {
    var include = compileKafkaTopics(USECASE_DIR.resolve("kafka-include-shared"), "package.json");
    var includingProject = compileKafkaTopics(USECASE_DIR.resolve(project), packageFile);

    assertThat(includingProject.included()).isNotEmpty().isSubsetOf(include.own());
    assertThat(includingProject.own()).doesNotContainAnyElementsOf(include.own());
  }

  private KafkaTopics compileKafkaTopics(Path useCase, String packageFile) throws IOException {
    var hook = snapshotExtension.execute(useCase, "compile", packageFile);
    assertThat(hook.isSuccess()).as(hook.getMessages()).isTrue();
    var plan =
        SqrlObjectMapper.INSTANCE.readTree(
            snapshotExtension.getPlanDir().resolve("kafka.json").toFile());
    // An included project's build directory would be copied into the build of the next project
    FileUtils.deleteDirectory(snapshotExtension.getBuildDir().toFile());
    return new KafkaTopics(
        topicNames(plan.path("topics")), topicNames(plan.path("includedTopics")));
  }

  private static Set<String> topicNames(JsonNode topics) {
    return StreamSupport.stream(topics.spliterator(), false)
        .map(topic -> topic.path("topicName").asText())
        .collect(Collectors.toSet());
  }

  /** The topics that a project writes itself, and those that it reads from standalone includes. */
  private record KafkaTopics(Set<String> own, Set<String> included) {}

  private String compileInsertIdentityUseCase() throws IOException {
    var useCase = USECASE_DIR.resolve("insert-table-identity-compile");
    var hook = snapshotExtension.execute(useCase, "compile", "package.json");
    assertThat(hook.isSuccess()).as(hook.getMessages()).isTrue();
    return Files.readString(snapshotExtension.getPlanDir().resolve("flink-sql-no-functions.sql"));
  }

  @Test
  void givenTagConfiguration_whenCompile_thenTagsAreWrittenToDeploymentAssets() throws IOException {
    var hook = snapshotExtension.execute(USECASE_DIR.resolve("banking"), "compile", "package.json");
    assertThat(hook.isSuccess()).as(hook.getMessages()).isTrue();

    var statements = readJdbcStatements("postgres.json");
    assertThat(statements).containsKeys("Customers", "CustomerNameCounts", "LoanTypes");
    // effective tags of tables and views, written as tag name -> value
    assertThat(statements.get("Customers"))
        .containsExactlyInAnyOrderEntriesOf(Map.of("pii", "raw", "classification", "confidential"));
    assertThat(statements.get("LoanTotals"))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("financial", "true", "classification", "confidential"));
    assertThat(statements.get("LoanTypeStatusCounts"))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("financial", "true", "classification", "internal"));
    assertThat(statements.get("CustomerNameCounts")).isNull();
    assertThat(statements.get("LoanTypes")).isNull();

    var explain = Files.readString(snapshotExtension.getBuildDir().resolve("pipeline_explain.txt"));
    assertThat(explain).contains("Tags:        classification=confidential, pii=raw");
  }

  @Test
  void givenNoTagConfiguration_whenCompile_thenTagsAreNotAnalyzed() throws IOException {
    var hook =
        snapshotExtension.execute(USECASE_DIR.resolve("sensors-doc"), "compile", "package.json");
    assertThat(hook.isSuccess()).as(hook.getMessages()).isTrue();

    assertThat(readJdbcStatements("postgres.json"))
        .isNotEmpty()
        .allSatisfy((name, tags) -> assertThat(tags).isNull());
    assertThat(Files.readString(snapshotExtension.getBuildDir().resolve("pipeline_explain.txt")))
        .doesNotContain("Tags:");
    assertThat(Files.readString(snapshotExtension.getBuildDir().resolve("pipeline_explain.json")))
        .doesNotContain("\"tags\"");
  }

  /** Reads the statements of a JDBC deployment plan, mapping the name to its tags */
  private Map<String, Map<String, String>> readJdbcStatements(String planFile) throws IOException {
    var plan =
        SqrlObjectMapper.INSTANCE.readValue(
            snapshotExtension.getPlanDir().resolve(planFile).toFile(), JdbcPlanModel.class);
    var result = new HashMap<String, Map<String, String>>();
    plan.statements().stream()
        .filter(
            s ->
                s.type() == JdbcStatementModel.Type.TABLE
                    || s.type() == JdbcStatementModel.Type.VIEW)
        .forEach(s -> result.put(s.logicalName() != null ? s.logicalName() : s.name(), s.tags()));
    return result;
  }

  @Test
  @Disabled("Intended for manual usage")
  void runTestCaseByName() {
    var pkg = USECASE_DIR.resolve("stdlib-openai").resolve("package.json");
    UseCaseTestHelper.testUseCase(
        snapshotExtension,
        getClass(),
        pkg,
        UseCaseTestHelper.defaultBuildDirFilter(),
        UseCaseTestHelper.defaultPlanDirFilter());
  }

  static class UseCaseFiles extends ArgumentsProviders.PackageProvider {
    UseCaseFiles() {
      super(USECASE_DIR);
    }
  }
}
