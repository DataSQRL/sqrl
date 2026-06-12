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

import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import java.util.List;
import org.junit.jupiter.api.Test;

class JdbcPhysicalPlanTest {

  private static GenericJdbcStatement stmt(String name, Type type, String sql) {
    return new GenericJdbcStatement(name, type, sql);
  }

  @Test
  void givenMixedStatements_whenGetDeploymentArtifacts_thenSchemaAndViewsArtifacts() {
    var plan =
        JdbcPhysicalPlan.builder()
            .statement(stmt("ext", Type.EXTENSION, "CREATE EXTENSION vector"))
            .statement(stmt("t1", Type.TABLE, "CREATE TABLE t1"))
            .statement(stmt("t2", Type.TABLE, "CREATE TABLE t2"))
            .statement(stmt("i1", Type.INDEX, "CREATE INDEX i1"))
            .statement(stmt("v1", Type.VIEW, "CREATE VIEW v1"))
            .tableIdMap(java.util.Map.of())
            .build();

    var artifacts = plan.getDeploymentArtifacts();

    assertThat(artifacts)
        .extracting(DeploymentArtifact::fileSuffix)
        .containsExactly("-schema.sql", "-views.sql");
    assertThat(artifacts.get(0).content())
        .isEqualTo(
            "CREATE EXTENSION vector;\n\nCREATE TABLE t1;\nCREATE TABLE t2;\n\nCREATE INDEX i1");
    assertThat(artifacts.get(1).content()).isEqualTo("CREATE VIEW v1");
  }

  @Test
  void givenExtraArtifacts_whenGetDeploymentArtifacts_thenAppendedAfterDefaults() {
    var plan =
        JdbcPhysicalPlan.builder()
            .statement(stmt("t1", Type.TABLE, "CREATE TABLE t1"))
            .tableIdMap(java.util.Map.of())
            .extraArtifact(new DeploymentArtifact("-partman.sql", "SELECT partman.create_parent"))
            .build();

    var artifacts = plan.getDeploymentArtifacts();

    assertThat(artifacts)
        .extracting(DeploymentArtifact::fileSuffix)
        .containsExactly("-schema.sql", "-views.sql", "-partman.sql");
    assertThat(artifacts.get(2).content()).isEqualTo("SELECT partman.create_parent");
  }

  @Test
  void givenJsonCreatorPlan_whenGetStatementsForType_thenFiltersByType() {
    var table = stmt("t1", Type.TABLE, "CREATE TABLE t1");
    var view = stmt("v1", Type.VIEW, "CREATE VIEW v1");
    var plan = new JdbcPhysicalPlan(List.of(table, view));

    assertThat(plan.getStatementsForType(Type.TABLE)).containsExactly(table);
    assertThat(plan.getStatementsForType(Type.INDEX)).isEmpty();
    assertThat(plan.getDeploymentArtifacts())
        .extracting(DeploymentArtifact::fileSuffix)
        .containsExactly("-schema.sql", "-views.sql");
  }
}
