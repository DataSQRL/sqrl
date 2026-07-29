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

import com.datasqrl.deployment.model.JdbcPlanModel;
import com.datasqrl.deployment.model.JdbcStatementModel;
import com.datasqrl.deployment.model.JdbcStatementModel.Type;
import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.pipeline.ExecutionStage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  @Override
  public JdbcPlanModel toFileModel() {
    return new JdbcPlanModel(
        statements.stream().map(JdbcPhysicalPlan::toStatementModel).toList(),
        standaloneExtensionStatements.stream().map(JdbcPhysicalPlan::toStatementModel).toList());
  }

  public List<JdbcStatement> getStatementsForType(Type type) {
    return statements.stream().filter(statement -> statement.getType() == type).toList();
  }

  public List<DeploymentArtifact> getDeploymentArtifacts() {
    var artifacts = new ArrayList<DeploymentArtifact>();
    artifacts.add(new DeploymentArtifact("-schema.sql", buildSchemaContent()));
    artifacts.add(new DeploymentArtifact("-views.sql", toSql(getStatementsForType(Type.VIEW))));
    standaloneExtensionStatements.stream()
        .map(
            statement ->
                new DeploymentArtifact("-" + statement.getName() + ".sql", toSql(statement)))
        .forEach(artifacts::add);
    return List.copyOf(artifacts);
  }

  private String buildSchemaContent() {
    return Stream.of(Type.EXTENSION, Type.TABLE, Type.INDEX)
        .map(this::getStatementsForType)
        .filter(Predicate.not(List::isEmpty))
        .map(JdbcPhysicalPlan::toSql)
        .collect(Collectors.joining(";\n\n"));
  }

  private static JdbcStatementModel toStatementModel(JdbcStatement statement) {
    var fields =
        statement.getFields() == null
            ? null
            : statement.getFields().stream()
                .map(
                    field ->
                        new JdbcStatementModel.Field(
                            field.name(), field.type(), field.nullable(), field.description()))
                .toList();
    if (statement instanceof CreateTableJdbcStatement createTable) {
      return new JdbcStatementModel(
          statement.getName(),
          statement.getType(),
          statement.getSql(),
          statement.getDescription(),
          fields,
          createTable.getPrimaryKey(),
          createTable.getPartitionKey(),
          createTable.getPartitionType(),
          createTable.getNumPartitions(),
          createTable.getTtl());
    }
    return new JdbcStatementModel(
        statement.getName(),
        statement.getType(),
        statement.getSql(),
        statement.getDescription(),
        fields);
  }

  private static String toSql(List<JdbcStatement> statements) {
    return DeploymentArtifact.toSqlString(statements.stream().map(JdbcStatement::getSql));
  }

  private static String toSql(JdbcStatement statement) {
    return DeploymentArtifact.toSqlString(statement.getSql());
  }
}
