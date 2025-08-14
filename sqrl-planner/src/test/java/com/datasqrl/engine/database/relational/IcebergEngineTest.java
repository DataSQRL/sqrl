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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IcebergEngineTest {

  @Mock private PackageJson mockPackageJson;

  @Mock private ConnectorFactoryFactory mockConnectorFactory;

  @Mock private ConnectorConf mockConnectorConf;

  private IcebergEngine icebergEngine;

  @BeforeEach
  void setUp() {
    var mockEngines = mock(PackageJson.EnginesConfig.class);
    var mockEngineConfig = mock(PackageJson.EngineConfig.class);

    when(mockPackageJson.getEngines()).thenReturn(mockEngines);
    when(mockEngines.getEngineConfigOrEmpty("iceberg")).thenReturn(mockEngineConfig);
    when(mockConnectorFactory.getOptionalConfig("iceberg"))
        .thenReturn(Optional.of(mockConnectorConf));

    icebergEngine = new IcebergEngine(mockPackageJson, mockConnectorFactory);
  }

  private void setupConnectorOptions(Map<String, String> options) {
    when(mockConnectorConf.toMapWithSubstitution(
            org.mockito.ArgumentMatchers.any(ConnectorConf.Context.class)))
        .thenReturn(options);
  }

  @Test
  void givenNonGlueCatalog_whenGetConnectorOptions_thenReturnsOriginalOptions() {
    setupConnectorOptions(
        Map.of(
            "catalog-table", "my_test_table",
            "catalog-impl", "org.apache.iceberg.hive.HiveCatalog"));

    var result = icebergEngine.getConnectorOptions("test", "test_id");

    assertThat(result.get("catalog-table")).isEqualTo("my_test_table");
  }

  @Test
  void givenNullCatalogImpl_whenGetConnectorOptions_thenReturnsOriginalOptions() {
    setupConnectorOptions(Map.of("catalog-table", "my_test_table"));

    var result = icebergEngine.getConnectorOptions("test", "test_id");

    assertThat(result.get("catalog-table")).isEqualTo("my_test_table");
  }

  @Test
  void givenGlueCatalogWithValidLowercase_whenGetConnectorOptions_thenReturnsLowercaseName() {
    setupConnectorOptions(
        Map.of(
            "catalog-table", "valid_table_123",
            "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"));

    var result = icebergEngine.getConnectorOptions("test", "test_id");

    assertThat(result.get("catalog-table")).isEqualTo("valid_table_123");
  }

  @Test
  void givenGlueCatalogWithValidMixedCase_whenGetConnectorOptions_thenReturnsLowercaseName() {
    setupConnectorOptions(
        Map.of(
            "catalog-table", "MyTable_123",
            "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"));

    var result = icebergEngine.getConnectorOptions("test", "test_id");

    assertThat(result.get("catalog-table")).isEqualTo("mytable_123");
  }

  @Test
  void givenGlueCatalogWithHyphens_whenGetConnectorOptions_thenThrowsIllegalArgumentException() {
    setupConnectorOptions(
        Map.of(
            "catalog-table", "my-table-name",
            "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"));

    assertThatThrownBy(() -> icebergEngine.getConnectorOptions("test", "test_id"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid AWS Glue table name: 'my-table-name'")
        .hasMessageContaining("Name has to match: ^[a-z0-9_]{1,255}$");
  }

  @Test
  void
      givenGlueCatalogWithSpecialChars_whenGetConnectorOptions_thenThrowsIllegalArgumentException() {
    setupConnectorOptions(
        Map.of(
            "catalog-table", "table@name!",
            "catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"));

    assertThatThrownBy(() -> icebergEngine.getConnectorOptions("test", "test_id"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid AWS Glue table name: 'table@name!'")
        .hasMessageContaining("Name has to match: ^[a-z0-9_]{1,255}$");
  }

  @Test
  void givenTableBuilder_whenGetConnectorTableName_thenReturnsTableNameFromOptions() {
    var tableBuilder = mock(FlinkTableBuilder.class);
    when(tableBuilder.getConnectorOptions()).thenReturn(Map.of("catalog-table", "simple_table"));

    var result = icebergEngine.getConnectorTableName(tableBuilder);

    assertThat(result).isEqualTo("simple_table");
  }
}
