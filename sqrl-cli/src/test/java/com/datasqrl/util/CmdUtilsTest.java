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

import com.datasqrl.deployment.model.FlinkPlanModel;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CmdUtilsTest {

  @TempDir private Path tempDir;

  @Test
  void givenPlanWithLocalIcebergSink_whenResetIceberg_thenDeletesOnlyTheSinkTable()
      throws IOException {
    var warehouse = tempDir.resolve("warehouse");
    var sinkTable = createTable(warehouse.resolve("default_database/_MyApplications1"));
    var sourceTable = createTable(warehouse.resolve("default_database/ExistingLakeTable"));
    var planDir =
        writeFlinkPlan(
            "CREATE TABLE `ExistingLakeTable` (`id` BIGINT) WITH ('connector' = 'iceberg',"
                + " 'catalog-type' = 'hadoop', 'warehouse' = '${WAREHOUSE}');",
            "CREATE TABLE `_MyApplications1_1` (`id` BIGINT) WITH ('connector' = 'iceberg',"
                + " 'catalog-type' = 'hadoop', 'warehouse' = '${WAREHOUSE}',"
                + " 'catalog-table' = '_MyApplications1');",
            "EXECUTE STATEMENT SET BEGIN INSERT INTO `_MyApplications1_1`"
                + " SELECT * FROM `ExistingLakeTable`; END");

    CmdUtils.resetIceberg(planDir, Map.of("WAREHOUSE", warehouse.toString()));

    assertThat(sinkTable).doesNotExist();
    assertThat(sourceTable.resolve("metadata/v1.metadata.json")).isRegularFile();
  }

  @Test
  void givenSinkTableNotCreatedYet_whenResetIceberg_thenDoesNothing() throws IOException {
    var warehouse = tempDir.resolve("warehouse");
    var planDir =
        writeFlinkPlan(
            "CREATE TABLE `sink` (`id` BIGINT) WITH ('connector' = 'iceberg',"
                + " 'catalog-type' = 'hadoop', 'warehouse' = '%s');".formatted(warehouse),
            "INSERT INTO `sink` SELECT 1;");

    assertThatCode(() -> CmdUtils.resetIceberg(planDir, Map.of())).doesNotThrowAnyException();
    assertThat(warehouse).doesNotExist();
  }

  @Test
  void givenWarehouseWithMissingEnvironmentVariable_whenResetIceberg_thenKeepsTheTable()
      throws IOException {
    var table = createTable(tempDir.resolve("warehouse/default_database/sink"));
    var planDir =
        writeFlinkPlan(
            "CREATE TABLE `sink` (`id` BIGINT) WITH ('connector' = 'iceberg',"
                + " 'catalog-type' = 'hadoop', 'warehouse' = '${MISSING_WAREHOUSE}');",
            "INSERT INTO `sink` SELECT 1;");

    assertThatCode(() -> CmdUtils.resetIceberg(planDir, Map.of())).doesNotThrowAnyException();
    assertThat(table).isDirectory();
  }

  @Test
  void givenPlanWithoutFlink_whenResetIceberg_thenDoesNothing() throws IOException {
    var planDir = Files.createDirectories(tempDir.resolve("plan"));

    assertThatCode(() -> CmdUtils.resetIceberg(planDir, Map.of())).doesNotThrowAnyException();
  }

  private Path writeFlinkPlan(String... flinkSql) throws IOException {
    var planDir = Files.createDirectories(tempDir.resolve("plan"));
    SqrlObjectMapper.INSTANCE.writeValue(
        planDir.resolve("flink.json").toFile(),
        new FlinkPlanModel(List.of(flinkSql), Set.of("iceberg"), Set.of(), Set.of()));
    return planDir;
  }

  private static Path createTable(Path tableDir) throws IOException {
    Files.createDirectories(tableDir.resolve("metadata"));
    Files.writeString(tableDir.resolve("metadata/v1.metadata.json"), "{}");
    return tableDir;
  }
}
