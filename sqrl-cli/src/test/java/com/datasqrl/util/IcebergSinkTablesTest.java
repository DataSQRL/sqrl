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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.flinkrunner.utils.EnvVarResolver;
import com.datasqrl.util.IcebergSinkTables.IcebergSinkTable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class IcebergSinkTablesTest {

  private static final UnaryOperator<String> NO_ENV = UnaryOperator.identity();

  @Test
  void givenPlanWithIcebergAndJdbcSinks_whenFind_thenReturnsOnlyTheIcebergInsertTargets() {
    var flinkSql =
        List.of(
            """
            CREATE TEMPORARY TABLE `_Applications__schema` (`id` BIGINT NOT NULL)
            WITH ('connector' = 'filesystem', 'format' = 'flexible-json',
              'path' = '${DATA_PATH}/applications.jsonl');""",
            """
            CREATE TABLE `_Applications` (PRIMARY KEY (`id`) NOT ENFORCED)
            WITH ('source.monitor-interval' = '1')
            LIKE `_Applications__schema`;""",
            "CREATE VIEW `_MyApplications1` AS SELECT * FROM `_Applications`;",
            """
            CREATE TABLE `_ExistingLakeTable` (`id` BIGINT NOT NULL)
            WITH ('connector' = 'iceberg', 'catalog-type' = 'hadoop', 'warehouse' = '/wh');""",
            """
            CREATE TABLE `_MyApplications1_1` (`id` BIGINT NOT NULL)
            PARTITIONED BY (`id`)
            WITH ('catalog-name' = 'default_catalog', 'catalog-table' = '_MyApplications1',
              'catalog-type' = 'hadoop', 'connector' = 'iceberg', 'warehouse' = '/tmp/duckdb');""",
            """
            CREATE TABLE `_MyApplications1_2` (`id` BIGINT NOT NULL)
            WITH ('connector' = 'jdbc-sqrl', 'table-name' = '_MyApplications1',
              'url' = 'jdbc:postgresql://${POSTGRES_AUTHORITY}');""",
            """
            EXECUTE STATEMENT SET BEGIN
            INSERT INTO `default_catalog`.`default_database`.`_MyApplications1_1`
            SELECT * FROM `default_catalog`.`default_database`.`_MyApplications1`;
            INSERT INTO `default_catalog`.`default_database`.`_MyApplications1_2`
            SELECT * FROM `default_catalog`.`default_database`.`_ExistingLakeTable`;
            END""");

    var sinks = IcebergSinkTables.find(flinkSql);

    assertThat(sinks).extracting(IcebergSinkTable::name).containsExactly("_MyApplications1_1");
    assertThat(sinks.get(0).options())
        .containsEntry("catalog-table", "_MyApplications1")
        .containsEntry("warehouse", "/tmp/duckdb");
  }

  @Test
  void givenExportSinkDefinedWithLike_whenFind_thenMergesOptionsOfTheLikeTable() {
    var flinkSql =
        List.of(
            """
            CREATE TEMPORARY TABLE `Iceberg_sink_ex1__schema` (`id` BIGINT NOT NULL)
            WITH ('connector' = 'datagen', 'number-of-rows' = '10');""",
            """
            CREATE TABLE `Iceberg_sink_ex1` (PRIMARY KEY (`id`) NOT ENFORCED)
            WITH ('catalog-table' = 'my-table', 'catalog-type' = 'hadoop',
              'connector' = 'iceberg', 'warehouse' = '${ICEBERG_WAREHOUSE}')
            LIKE `Iceberg_sink_ex1__schema`;""",
            """
            EXECUTE STATEMENT SET BEGIN
            INSERT INTO `default_catalog`.`default_database`.`Iceberg_sink_ex1`
            SELECT * FROM `default_catalog`.`default_database`.`Iceberg_sink_ex1__schema`;
            END""");

    var sinks = IcebergSinkTables.find(flinkSql);

    assertThat(sinks).extracting(IcebergSinkTable::name).containsExactly("Iceberg_sink_ex1");
    assertThat(sinks.get(0).options())
        .containsEntry("connector", "iceberg")
        .containsEntry("warehouse", "${ICEBERG_WAREHOUSE}")
        .containsEntry("number-of-rows", "10");
  }

  @Test
  void givenSinkInheritingTheIcebergConnector_whenFind_thenReturnsIt() {
    var flinkSql =
        List.of(
            """
            CREATE TEMPORARY TABLE `base` (`id` BIGINT)
            WITH ('connector' = 'iceberg', 'catalog-type' = 'hadoop', 'warehouse' = '/wh');""",
            "CREATE TABLE `sink` WITH ('catalog-table' = 't') LIKE `base`;",
            "INSERT INTO `sink` SELECT * FROM `base`;");

    assertThat(IcebergSinkTables.find(flinkSql))
        .singleElement()
        .satisfies(
            sink ->
                assertThat(sink.options())
                    .containsEntry("connector", "iceberg")
                    .containsEntry("catalog-table", "t"));
  }

  @Test
  void givenLikeExcludingOptions_whenFind_thenDoesNotInheritTheIcebergConnector() {
    var flinkSql =
        List.of(
            "CREATE TEMPORARY TABLE `base` (`id` BIGINT) WITH ('connector' = 'iceberg');",
            "CREATE TABLE `sink` WITH ('connector' = 'print') LIKE `base` (EXCLUDING OPTIONS);",
            "INSERT INTO `sink` SELECT * FROM `base`;");

    assertThat(IcebergSinkTables.find(flinkSql)).isEmpty();
  }

  @Test
  void givenSameTableNameInTwoDatabases_whenFind_thenResolvesInsertTargetsByUseStatements() {
    var flinkSql =
        List.of(
            "CREATE DATABASE IF NOT EXISTS `sources`;",
            "USE `sources`;",
            "CREATE TABLE `Orders` (`id` BIGINT) WITH ('connector' = 'iceberg');",
            "USE CATALOG `default_catalog`;",
            "USE `default_database`;",
            "CREATE TABLE `Orders` (`id` BIGINT) WITH ('connector' = 'print');",
            """
            EXECUTE STATEMENT SET BEGIN
            INSERT INTO `default_catalog`.`default_database`.`Orders`
            SELECT * FROM `default_catalog`.`sources`.`Orders`;
            END""");

    assertThat(IcebergSinkTables.find(flinkSql)).isEmpty();
  }

  @Test
  void givenStatementThatCannotBeParsed_whenFind_thenSkipsIt() {
    var flinkSql =
        List.of(
            "THIS IS NOT FLINK SQL;",
            "CREATE TABLE `sink` (`id` BIGINT) WITH ('connector' = 'iceberg');",
            "INSERT INTO `sink` SELECT 1;");

    assertThat(IcebergSinkTables.find(flinkSql))
        .extracting(IcebergSinkTable::name)
        .containsExactly("sink");
  }

  @Test
  void givenHadoopCatalogWithLocalWarehouse_whenLocalDirectory_thenReturnsTheTableDirectory() {
    var table =
        new IcebergSinkTable(
            "_MyApplications1_1",
            Map.of(
                "catalog-type", "hadoop",
                "warehouse", "/tmp/duckdb",
                "catalog-table", "_MyApplications1"));

    assertThat(table.localDirectory(NO_ENV))
        .contains(Path.of("/tmp/duckdb/default_database/_MyApplications1"));
  }

  @Test
  void givenNoCatalogTableOrDatabase_whenLocalDirectory_thenUsesFlinkTableAndCatalogDatabase() {
    var table =
        new IcebergSinkTable(
            "sink",
            Map.of(
                "catalog-type", "HADOOP",
                "warehouse", "file:///tmp/wh",
                "catalog-database", "lake"));

    assertThat(table.localDirectory(NO_ENV)).contains(Path.of("/tmp/wh/lake/sink"));
  }

  @Test
  void givenRelativeWarehouse_whenLocalDirectory_thenResolvesAgainstWorkingDirectory() {
    var table =
        new IcebergSinkTable("sink", Map.of("catalog-type", "hadoop", "warehouse", "warehouse"));

    assertThat(table.localDirectory(NO_ENV))
        .contains(Path.of("warehouse/default_database/sink").toAbsolutePath());
  }

  @Test
  void givenWarehouseFromEnvironment_whenLocalDirectory_thenResolvesTheVariable() {
    var table =
        new IcebergSinkTable(
            "sink", Map.of("catalog-type", "hadoop", "warehouse", "${ICEBERG_WAREHOUSE}"));
    var resolver =
        EnvVarResolver.builder().envVars(Map.of("ICEBERG_WAREHOUSE", "/data/wh")).build();

    assertThat(table.localDirectory(resolver::resolve))
        .contains(Path.of("/data/wh/default_database/sink"));
  }

  @Test
  void givenMissingEnvironmentVariable_whenLocalDirectory_thenThrows() {
    var table =
        new IcebergSinkTable(
            "sink", Map.of("catalog-type", "hadoop", "warehouse", "${ICEBERG_WAREHOUSE}"));
    var resolver = EnvVarResolver.builder().envVars(Map.of()).build();

    assertThatThrownBy(() -> table.localDirectory(resolver::resolve))
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
  void givenWarehouseThatIsNotLocal_whenLocalDirectory_thenReturnsEmpty(String warehouse) {
    var table =
        new IcebergSinkTable("sink", Map.of("catalog-type", "hadoop", "warehouse", warehouse));

    assertThat(table.localDirectory(NO_ENV)).isEmpty();
  }

  @Test
  void givenNonHadoopCatalog_whenLocalDirectory_thenReturnsEmpty() {
    var glue =
        new IcebergSinkTable(
            "sink",
            Map.of(
                "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog",
                "catalog-type", "hadoop",
                "warehouse", "/wh"));
    var hive = new IcebergSinkTable("sink", Map.of("catalog-type", "hive", "warehouse", "/wh"));
    var defaultType = new IcebergSinkTable("sink", Map.of("warehouse", "/wh"));

    assertThat(glue.localDirectory(NO_ENV)).isEmpty();
    assertThat(hive.localDirectory(NO_ENV)).isEmpty();
    assertThat(defaultType.localDirectory(NO_ENV)).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"..", "../other", "nested/table", "", "/abs"})
  void givenTableNameThatIsAPath_whenLocalDirectory_thenReturnsEmpty(String catalogTable) {
    var table =
        new IcebergSinkTable(
            "sink",
            Map.of("catalog-type", "hadoop", "warehouse", "/wh", "catalog-table", catalogTable));

    assertThat(table.localDirectory(NO_ENV)).isEmpty();
  }
}
