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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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
    @JsonIgnore ExecutionStage stage,
    @Singular List<JdbcStatement> statements,
    @JsonIgnore @Singular List<RelNode> queries,
    @JsonIgnore Map<String, JdbcEngineCreateTable> tableIdMap)
    implements DatabasePhysicalPlan {

  @SuppressWarnings("unused")
  @JsonCreator
  public JdbcPhysicalPlan(@JsonProperty("statements") List<JdbcStatement> statements) {
    this(null, new ArrayList<>(statements), List.of(), Map.of());
  }

  public List<JdbcStatement> getStatementsForType(Type type) {
    return statements.stream().filter(s -> s.getType() == type).collect(Collectors.toList());
  }

  private static String toSql(List<JdbcStatement> statements) {
    return DeploymentArtifact.toSqlString(statements.stream().map(JdbcStatement::getSql));
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
