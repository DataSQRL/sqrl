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
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ViewStatementIdentifierTest {

  @Test
  void givenDefaultStatementFactory_whenGettingViewIdentifier_thenReturnsViewNameOnly() {
    var factory = new PostgresStatementFactory(1);

    assertThat(factory.getViewStatementIdentifier("Orders").names).containsExactly("Orders");
  }

  @Test
  void givenSparkViewLocation_whenGettingViewIdentifier_thenPrependsCatalogAndDatabase() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getSettingOptional("view-catalog")).thenReturn(Optional.of("spark_catalog"));
    when(engineConfig.getSettingOptional("view-database")).thenReturn(Optional.of("analytics"));
    var factory = new SparkSqlStatementFactory(engineConfig);

    assertThat(factory.getViewStatementIdentifier("Orders").names)
        .containsExactly("spark_catalog", "analytics", "Orders");
  }

  @Test
  void givenSparkViewCatalogWithoutDatabase_whenGettingViewIdentifier_thenUsesDefaultDatabase() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getSettingOptional("view-catalog")).thenReturn(Optional.of("spark_catalog"));
    when(engineConfig.getSettingOptional("view-database")).thenReturn(Optional.empty());
    var factory = new SparkSqlStatementFactory(engineConfig);

    assertThat(factory.getViewStatementIdentifier("Orders").names)
        .containsExactly("spark_catalog", "default", "Orders");
  }

  @Test
  void givenRedshiftViewLocation_whenGettingViewIdentifier_thenPrependsDatabaseAndSchema() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getSettingOptional("view-database")).thenReturn(Optional.of("analytics"));
    when(engineConfig.getSettingOptional("view-schema")).thenReturn(Optional.of("reporting"));
    var factory = new RedshiftStatementFactory(engineConfig);

    assertThat(factory.getViewStatementIdentifier("Orders").names)
        .containsExactly("analytics", "reporting", "Orders");
  }

  @Test
  void givenRedshiftViewDatabaseWithoutSchema_whenGettingViewIdentifier_thenUsesPublicSchema() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getSettingOptional("view-database")).thenReturn(Optional.of("analytics"));
    when(engineConfig.getSettingOptional("view-schema")).thenReturn(Optional.empty());
    var factory = new RedshiftStatementFactory(engineConfig);

    assertThat(factory.getViewStatementIdentifier("Orders").names)
        .containsExactly("analytics", "public", "Orders");
  }
}
