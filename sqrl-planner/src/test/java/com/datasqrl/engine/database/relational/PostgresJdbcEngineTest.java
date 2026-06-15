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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.PostgresCreateTableDdlFactory;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.function.translation.postgres.extensions.PgPartmanExtension;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresJdbcEngineTest {

  @Mock private PackageJson mockPackageJson;

  @Mock private ConnectorFactoryFactory mockConnectorFactory;

  @Mock private ExecutionStage mockStage;

  @BeforeEach
  void setUp() {
    var mockEngines = mock(PackageJson.EnginesConfig.class);
    var mockEngineConfig = mock(PackageJson.EngineConfig.class);

    when(mockPackageJson.getEngines()).thenReturn(mockEngines);
    when(mockEngines.getEngineConfigOrEmpty("postgres")).thenReturn(mockEngineConfig);
  }

  @Test
  void givenNoPartmanTables_whenPlan_thenNoPartmanArtifact() {
    var engine = new PostgresJdbcEngine(mockPackageJson, mockConnectorFactory);
    var stagePlan = MaterializationStagePlan.builder().stage(mockStage).build();

    var plan = (JdbcPhysicalPlan) engine.plan(stagePlan);

    assertThat(plan.getDeploymentArtifacts())
        .extracting(DeploymentArtifact::fileSuffix)
        .containsExactly("-schema.sql", "-views.sql");
  }

  @Test
  void givenRangeTtlTable_whenPlan_thenPartmanExtensionAddedToSchema() {
    var flinkTable = mock(FlinkTableBuilder.class);
    when(flinkTable.getTableName()).thenReturn("orders_1");
    var engineTable = new JdbcEngineCreateTable("orders_1", flinkTable, null, null);

    var createStmt =
        new CreateTableJdbcStatement(
            "orders_1",
            null,
            List.of(),
            List.of("id", "ts"),
            List.of("ts"),
            PartitionType.RANGE,
            1,
            Duration.ofDays(30),
            engineTable,
            new PostgresCreateTableDdlFactory(true));

    var stmtFactory = mock(JdbcStatementFactory.class);
    when(stmtFactory.createTable(engineTable)).thenReturn(createStmt);
    when(stmtFactory.applyTableExtensions(any()))
        .thenReturn(
            List.of(
                new GenericJdbcStatement(
                    "PgPartmanExtension",
                    Type.EXTENSION,
                    new PgPartmanExtension().getDdl(List.of(createStmt)))));
    when(stmtFactory.supportsQueries()).thenReturn(false);

    var engine =
        new PostgresJdbcEngine(mockPackageJson, mockConnectorFactory) {
          @Override
          public JdbcStatementFactory getStatementFactory() {
            return stmtFactory;
          }
        };
    var stagePlan = MaterializationStagePlan.builder().stage(mockStage).table(engineTable).build();

    var plan = (JdbcPhysicalPlan) engine.plan(stagePlan);

    var artifacts = plan.getDeploymentArtifacts();
    assertThat(artifacts)
        .extracting(DeploymentArtifact::fileSuffix)
        .containsExactly("-schema.sql", "-views.sql");
    assertThat((String) artifacts.get(0).content())
        .contains("partman.create_parent")
        .contains("retention            = '30 days'");
  }
}
