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
package com.datasqrl.engine.database.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

class ViewStatementIdentifierTest {

  @Test
  void givenDefaultStatementFactory_whenGettingViewIdentifier_thenReturnsViewNameOnly() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyAs(
            PostgresStatementFactory.PARTITION_TTL_DIVISOR_KEY, Integer.class))
        .thenReturn(1);
    var factory = new PostgresStatementFactory(engineConfig);

    assertThat(factory.getCreateViewDdlFactory().getViewIdentifier("Orders").names)
        .containsExactly("Orders");
  }

  @Test
  void givenSparkViewLocation_whenGettingViewIdentifier_thenPrependsCatalogAndDatabase() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyOptional("view-catalog")).thenReturn(Optional.of("spark_catalog"));
    when(engineConfig.getPropertyOptional("view-database")).thenReturn(Optional.of("analytics"));
    var factory = new SparkSqlStatementFactory(engineConfig);

    assertThat(factory.getCreateViewDdlFactory().getViewIdentifier("Orders").names)
        .containsExactly("spark_catalog", "analytics", "Orders");
  }

  @Test
  void givenSparkViewCatalogWithoutDatabase_whenGettingViewIdentifier_thenUsesDefaultDatabase() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyOptional("view-catalog")).thenReturn(Optional.of("spark_catalog"));
    when(engineConfig.getPropertyOptional("view-database")).thenReturn(Optional.empty());
    var factory = new SparkSqlStatementFactory(engineConfig);

    assertThat(factory.getCreateViewDdlFactory().getViewIdentifier("Orders").names)
        .containsExactly("spark_catalog", "default", "Orders");
  }

  @Test
  void givenRedshiftViewLocation_whenGettingViewIdentifier_thenPrependsDatabaseAndSchema() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyOptional("view-database")).thenReturn(Optional.of("analytics"));
    when(engineConfig.getPropertyOptional("view-schema")).thenReturn(Optional.of("reporting"));
    var factory = new RedshiftStatementFactory(engineConfig);

    assertThat(factory.getCreateViewDdlFactory().getViewIdentifier("Orders").names)
        .containsExactly("analytics", "reporting", "Orders");
  }

  @Test
  void givenRedshiftViewDatabaseWithoutSchema_whenGettingViewIdentifier_thenUsesPublicSchema() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyOptional("view-database")).thenReturn(Optional.of("analytics"));
    when(engineConfig.getPropertyOptional("view-schema")).thenReturn(Optional.empty());
    var factory = new RedshiftStatementFactory(engineConfig);

    assertThat(factory.getCreateViewDdlFactory().getViewIdentifier("Orders").names)
        .containsExactly("analytics", "public", "Orders");
  }

  @Test
  void givenIcebergTable_whenGettingRedshiftSourceIdentifier_thenUsesGlueCatalogOptions() {
    var engineConfig = mock(EngineConfig.class);
    var tableBuilder = mock(FlinkTableBuilder.class);
    when(tableBuilder.getConnectorOptions())
        .thenReturn(
            Map.of(
                "catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog",
                "catalog-database",
                "sqrl",
                "catalog-table",
                "deployment_orders"));
    var table = new JdbcEngineCreateTable("FlinkOrders", tableBuilder, null, null);
    var factory = new RedshiftStatementFactory(engineConfig);

    assertThat(factory.getTableNameMapping(Map.of("FlinkOrders", table)).get("FlinkOrders").names)
        .containsExactly("awsdatacatalog", "sqrl", "deployment_orders");
  }

  @Test
  void givenHadoopIcebergTable_whenGettingRedshiftSourceIdentifier_thenUsesCatalogName() {
    var engineConfig = mock(EngineConfig.class);
    var tableBuilder = mock(FlinkTableBuilder.class);
    when(tableBuilder.getConnectorOptions())
        .thenReturn(
            Map.of(
                "catalog-name", "hadoop_catalog",
                "catalog-database", "sqrl",
                "catalog-table", "deployment_orders"));
    var table = new JdbcEngineCreateTable("FlinkOrders", tableBuilder, null, null);
    var factory = new RedshiftStatementFactory(engineConfig);

    assertThat(factory.getTableNameMapping(Map.of("FlinkOrders", table)).get("FlinkOrders").names)
        .containsExactly("hadoop_catalog", "sqrl", "deployment_orders");
  }

  @Test
  void givenRedshiftView_whenCreatingView_thenUsesLateBinding() throws Exception {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyOptional("view-database")).thenReturn(Optional.empty());
    when(engineConfig.getPropertyOptional("view-schema")).thenReturn(Optional.empty());
    var factory = new RedshiftStatementFactory(engineConfig);

    assertThat(
            factory
                .getCreateViewDdlFactory()
                .createView(
                    new SqlIdentifier("Orders", SqlParserPos.ZERO),
                    List.of("id"),
                    "SELECT id FROM \"sqrl\".\"orders\""))
        .isEqualTo(
            "CREATE OR REPLACE VIEW \"Orders\"(\"id\") AS SELECT id FROM \"sqrl\".\"orders\" WITH NO SCHEMA BINDING");
  }

  @Test
  void givenTrinoViewLocation_whenGettingViewIdentifier_thenPrependsCatalogAndSchema() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyOptional("view-catalog")).thenReturn(Optional.of("analytics"));
    when(engineConfig.getPropertyOptional("view-schema")).thenReturn(Optional.of("reporting"));
    var factory = new TrinoStatementFactory(engineConfig);

    assertThat(factory.getCreateViewDdlFactory().getViewIdentifier("Orders").names)
        .containsExactly("analytics", "reporting", "Orders");
  }

  @Test
  void givenTrinoViewCatalogWithoutSchema_whenGettingViewIdentifier_thenUsesPublicSchema() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyOptional("view-catalog")).thenReturn(Optional.of("analytics"));
    when(engineConfig.getPropertyOptional("view-schema")).thenReturn(Optional.empty());
    var factory = new TrinoStatementFactory(engineConfig);

    assertThat(factory.getCreateViewDdlFactory().getViewIdentifier("Orders").names)
        .containsExactly("analytics", "public", "Orders");
  }
}
