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
package com.datasqrl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.deployment.model.IcebergPlanModel;
import com.datasqrl.deployment.model.IcebergTableModel;
import com.datasqrl.flinkrunner.utils.EnvVarResolver;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CmdUtilsTest {

  private static final UnaryOperator<String> NO_ENV = UnaryOperator.identity();

  @TempDir private Path tempDir;

  @Test
  void givenPlanWithLocalIcebergTable_whenResetIceberg_thenDeletesOnlyThatTable()
      throws IOException {
    var warehouse = tempDir.resolve("warehouse");
    var planTable = createTable(warehouse.resolve("default_database/_MyApplications1"));
    var otherTable = createTable(warehouse.resolve("default_database/ExistingLakeTable"));
    var planDir = writeIcebergPlan(hadoopTable("_MyApplications1", "${WAREHOUSE}"));

    CmdUtils.resetIceberg(planDir, Map.of("WAREHOUSE", warehouse.toString()));

    assertThat(planTable).doesNotExist();
    assertThat(otherTable.resolve("metadata/v1.metadata.json")).isRegularFile();
  }

  @Test
  void givenTableNotCreatedYet_whenResetIceberg_thenDoesNothing() throws IOException {
    var warehouse = tempDir.resolve("warehouse");
    var planDir = writeIcebergPlan(hadoopTable("sink", warehouse.toString()));

    assertThatCode(() -> CmdUtils.resetIceberg(planDir, Map.of())).doesNotThrowAnyException();
    assertThat(warehouse).doesNotExist();
  }

  @Test
  void givenWarehouseWithMissingEnvironmentVariable_whenResetIceberg_thenKeepsTheTable()
      throws IOException {
    var table = createTable(tempDir.resolve("warehouse/default_database/sink"));
    var planDir = writeIcebergPlan(hadoopTable("sink", "${MISSING_WAREHOUSE}"));

    assertThatCode(() -> CmdUtils.resetIceberg(planDir, Map.of())).doesNotThrowAnyException();
    assertThat(table).isDirectory();
  }

  @Test
  void givenPlanWithoutIceberg_whenResetIceberg_thenDoesNothing() throws IOException {
    var planDir = Files.createDirectories(tempDir.resolve("plan"));

    assertThatCode(() -> CmdUtils.resetIceberg(planDir, Map.of())).doesNotThrowAnyException();
  }

  @Test
  void givenHadoopCatalogWithLocalWarehouse_whenLocalTableDir_thenReturnsTheTableDirectory() {
    var table =
        new IcebergTableModel("_MyApplications1", "default_database", "HADOOP", null, "/tmp/wh");

    assertThat(CmdUtils.localTableDir(table, NO_ENV))
        .contains(Path.of("/tmp/wh/default_database/_MyApplications1"));
  }

  @Test
  void givenFileUriWarehouse_whenLocalTableDir_thenReturnsTheTableDirectory() {
    var table = new IcebergTableModel("sink", "lake", "hadoop", null, "file:///tmp/wh");

    assertThat(CmdUtils.localTableDir(table, NO_ENV)).contains(Path.of("/tmp/wh/lake/sink"));
  }

  @Test
  void givenRelativeWarehouse_whenLocalTableDir_thenResolvesAgainstWorkingDirectory() {
    var table = hadoopTable("sink", "warehouse");

    assertThat(CmdUtils.localTableDir(table, NO_ENV))
        .contains(Path.of("warehouse/default_database/sink").toAbsolutePath());
  }

  @Test
  void givenWarehouseFromEnvironment_whenLocalTableDir_thenResolvesTheVariable() {
    var table = hadoopTable("sink", "${ICEBERG_WAREHOUSE}");
    var resolver =
        EnvVarResolver.builder().envVars(Map.of("ICEBERG_WAREHOUSE", "/data/wh")).build();

    assertThat(CmdUtils.localTableDir(table, resolver::resolve))
        .contains(Path.of("/data/wh/default_database/sink"));
  }

  @Test
  void givenMissingEnvironmentVariable_whenLocalTableDir_thenThrows() {
    var table = hadoopTable("sink", "${ICEBERG_WAREHOUSE}");
    var resolver = EnvVarResolver.builder().envVars(Map.of()).build();

    assertThatThrownBy(() -> CmdUtils.localTableDir(table, resolver::resolve))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ICEBERG_WAREHOUSE");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "s3://bucket/warehouse",
        "s3a://bucket/warehouse",
        "hdfs://namenode:8020/warehouse",
        "file:relative",
        "${UNRESOLVED}",
        " "
      })
  void givenWarehouseThatIsNotLocal_whenLocalTableDir_thenReturnsEmpty(String warehouse) {
    assertThat(CmdUtils.localTableDir(hadoopTable("sink", warehouse), NO_ENV)).isEmpty();
  }

  @Test
  void givenNonHadoopCatalog_whenLocalTableDir_thenReturnsEmpty() {
    var glue =
        new IcebergTableModel(
            "sink", "default_database", null, "org.apache.iceberg.aws.glue.GlueCatalog", "/wh");
    var hive = new IcebergTableModel("sink", "default_database", "hive", null, "/wh");
    var noWarehouse = new IcebergTableModel("sink", "default_database", "hadoop", null, null);

    assertThat(CmdUtils.localTableDir(glue, NO_ENV)).isEmpty();
    assertThat(CmdUtils.localTableDir(hive, NO_ENV)).isEmpty();
    assertThat(CmdUtils.localTableDir(noWarehouse, NO_ENV)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"..", "../other", "nested/table", "", "/abs"})
  void givenTableNameThatIsAPath_whenLocalTableDir_thenReturnsEmpty(String tableName) {
    assertThat(CmdUtils.localTableDir(hadoopTable(tableName, "/wh"), NO_ENV)).isEmpty();
  }

  private static IcebergTableModel hadoopTable(String tableName, String warehouse) {
    return new IcebergTableModel(tableName, "default_database", "hadoop", null, warehouse);
  }

  private Path writeIcebergPlan(IcebergTableModel... tables) throws IOException {
    var planDir = Files.createDirectories(tempDir.resolve("plan"));
    SqrlObjectMapper.INSTANCE.writeValue(
        planDir.resolve("iceberg.json").toFile(), new IcebergPlanModel(List.of(tables)));
    return planDir;
  }

  private static Path createTable(Path tableDir) throws IOException {
    Files.createDirectories(tableDir.resolve("metadata"));
    Files.writeString(tableDir.resolve("metadata/v1.metadata.json"), "{}");
    return tableDir;
  }
}
