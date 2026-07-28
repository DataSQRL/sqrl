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

import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;
import org.apache.calcite.rel.RelNode;

/**
 * @param queries Queries that are used for index selection
 * @param tableIdMap The original {@link JdbcEngineCreateTable} definitions so we can extract the
 *     mappings from table names and ids
 */
@Builder(toBuilder = true)
public record JdbcPhysicalPlan(
    ExecutionStage stage,
    @Singular List<JdbcStatement> statements,
    @Singular List<JdbcStatement> standaloneExtensionStatements,
    @Singular List<RelNode> queries,
    Map<String, CreateTableJdbcStatement> tableIdMap)
    implements DatabasePhysicalPlan {

  @JsonValue
  public JdbcPlan toModel() {
    return new JdbcPlan(statements, standaloneExtensionStatements);
  }

  public List<JdbcStatement> getStatementsForType(Type type) {
    return toModel().getStatementsForType(type);
  }

  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    return toModel().getDeploymentArtifacts();
  }
}
