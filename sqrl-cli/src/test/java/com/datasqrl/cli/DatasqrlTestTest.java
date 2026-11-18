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
package com.datasqrl.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.cli.output.NoOutputFormatter;
import com.datasqrl.cli.output.TestOutputManager;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.PackageJson;
import com.datasqrl.deployment.model.FlinkPlanModel;
import com.datasqrl.deployment.model.JdbcStatementModel;
import com.datasqrl.engine.database.relational.GenericJdbcStatement;
import com.datasqrl.util.SqrlObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasqrlTestTest {

  @TempDir private Path tempDir;

  @Test
  void requiresSnapshotValidation_whenSnapshotsWereCreatedAndAllOtherSnapshotsMatch_returnsTrue() {
    assertThat(
            DatasqrlTest.requiresSnapshotValidation(
                List.of(
                    new TestResult.SnapshotOk("existing"), new TestResult.SnapshotCreate("new"))))
        .isTrue();
  }

  @Test
  void requiresSnapshotValidation_whenNoSnapshotWasCreated_returnsFalse() {
    assertThat(
            DatasqrlTest.requiresSnapshotValidation(
                List.of(
                    new TestResult.SnapshotOk("existing"),
                    new TestResult.SnapshotMismatch("changed", "old", "new"))))
        .isFalse();
  }

  @Test
  void requiresSnapshotValidation_whenExistingSnapshotMismatches_returnsFalse() {
    assertThat(
            DatasqrlTest.requiresSnapshotValidation(
                List.of(
                    new TestResult.SnapshotCreate("new"),
                    new TestResult.SnapshotMismatch("changed", "old", "new"))))
        .isFalse();
  }

  @Test
  void run_whenPipelineFailsToStart_recordsFailureWithNonZeroExit() throws Exception {
    var planDir = tempDir.resolve("plan");
    Files.createDirectories(planDir);
    // The compiled plan references an environment variable that is not provided. The runner throws
    // a clear IllegalStateException, which must be reported as a test failure rather than
    // swallowed.
    Files.writeString(
        planDir.resolve("flink-sql.sql"),
        "CREATE TABLE t (id INT) WITH ('connector' = 'datagen', 'id' = '${NON_EXISTING_ENV_VAR}');\n");

    var exitCode = newDatasqrlTest(planDir).run();
    assertThat(exitCode).isNotZero();
  }

  @Test
  void givenIcebergSinkLeftByEarlierInvocation_whenRun_thenDeletesItBeforeThePipelineStarts()
      throws Exception {
    var planDir = Files.createDirectories(tempDir.resolve("plan"));
    var warehouse = tempDir.resolve("warehouse");
    var leftoverTable =
        Files.createDirectories(warehouse.resolve("default_database/MyTable/metadata"));
    Files.writeString(leftoverTable.resolve("v1.metadata.json"), "{}");
    SqrlObjectMapper.INSTANCE.writeValue(
        planDir.resolve("flink.json").toFile(),
        new FlinkPlanModel(
            List.of(
                "CREATE TABLE `MyTable_1` (`id` INT) WITH ('connector' = 'iceberg',"
                    + " 'catalog-type' = 'hadoop', 'catalog-table' = 'MyTable',"
                    + " 'warehouse' = '%s');".formatted(warehouse),
                "INSERT INTO `MyTable_1` SELECT 1;"),
            Set.of("iceberg"),
            Set.of(),
            Set.of()));
    // Fails the pipeline right away, the reset must have happened before it starts
    Files.writeString(
        planDir.resolve("flink-sql.sql"),
        "CREATE TABLE t (id INT) WITH ('connector' = 'datagen', 'id' = '${NON_EXISTING_ENV_VAR}');\n");

    var exitCode = newDatasqrlTest(planDir).run();

    assertThat(exitCode).isNotZero();
    assertThat(leftoverTable.getParent()).doesNotExist();
    assertThat(warehouse.resolve("default_database")).isDirectory();
  }

  private DatasqrlTest newDatasqrlTest(Path planDir) {
    var sqrlConfig = mock(PackageJson.class, RETURNS_DEEP_STUBS);
    when(sqrlConfig.getCompilerConfig().compileFlinkPlan()).thenReturn(false);
    when(sqrlConfig.getTestConfig().getSnapshotDir(any())).thenReturn(tempDir.resolve("snapshots"));
    when(sqrlConfig.getTestConfig().getTestDir(any())).thenReturn(Optional.empty());
    when(sqrlConfig.getTestConfig().getDelaySec()).thenReturn(5);
    when(sqrlConfig.getTestConfig().getMutationDelaySec()).thenReturn(0);
    when(sqrlConfig.getTestConfig().getRequiredCheckpoints()).thenReturn(0);

    return new DatasqrlTest(
        tempDir,
        planDir,
        sqrlConfig,
        new Configuration(),
        Map.of(),
        new TestOutputManager(tempDir),
        new NoOutputFormatter());
  }

  @Test
  void givenTestPlanWithJdbcViews_whenDeserialized_thenUsesGenericJdbcStatements()
      throws Exception {
    var json =
        """
        {
          "jdbcViews": [{
            "name": "orders_view",
            "type": "VIEW",
            "sql": "CREATE VIEW orders_view AS SELECT 1"
          }],
          "queries": [],
          "mutations": [],
          "subscriptions": []
        }
        """;

    var plan = SqrlObjectMapper.INSTANCE.readValue(json, TestPlan.class);

    assertThat(plan.getJdbcViews()).hasSize(1);
    assertThat(plan.getJdbcViews().get(0)).isInstanceOf(GenericJdbcStatement.class);
    assertThat(plan.getJdbcViews().get(0).getType()).isEqualTo(JdbcStatementModel.Type.VIEW);
  }
}
