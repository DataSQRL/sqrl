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
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
@Builder(toBuilder = true)
public class JdbcPhysicalPlan implements DatabasePhysicalPlan {

  @JsonIgnore ExecutionStage stage;
  @Singular List<JdbcStatement> statements;

  /**
   * Queries that are used for index selection
   *
   * @param type
   * @return
   */
  @JsonIgnore @Singular List<RelNode> queries;

  /**
   * The original {@link JdbcEngineCreateTable} definitions so we can extract the mappings from
   * table names and ids
   */
  @JsonIgnore List<JdbcEngineCreateTable> createTables;

  public List<JdbcStatement> getStatementsForType(JdbcStatement.Type type) {
    return statements.stream().filter(s -> s.getType() == type).collect(Collectors.toList());
  }

  private static String toSql(List<JdbcStatement> statements) {
    return DeploymentArtifact.toSqlString(statements.stream().map(JdbcStatement::getSql));
  }

  @JsonIgnore
  public Map<String, TableAnalysis> getTableId2AnalysisMap() {
    return createTables.stream()
        .collect(
            Collectors.toMap(
                createTable -> createTable.getTable().getTableName(),
                JdbcEngineCreateTable::getTableAnalysis));
  }

  @JsonIgnore
  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    return List.of(
        new DeploymentArtifact(
            "-schema.sql",
            Stream.of(Type.EXTENSION, Type.TABLE, Type.INDEX)
                .map(this::getStatementsForType)
                .filter(Predicate.not(List::isEmpty))
                .map(JdbcPhysicalPlan::toSql)
                .collect(Collectors.joining(";\n\n"))),
        new DeploymentArtifact("-views.sql", toSql(getStatementsForType(Type.VIEW))));
  }
}
