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
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Singular;

/** The serialized model of the JDBC database plan files (e.g. {@code postgres.json}). */
@Builder
public record JdbcPlan(
    @Singular List<JdbcStatement> statements,
    @Singular List<JdbcStatement> standaloneExtensionStatements)
    implements DatabasePhysicalPlan {

  public JdbcPlan {
    statements = statements == null ? List.of() : List.copyOf(statements);
    standaloneExtensionStatements =
        standaloneExtensionStatements == null
            ? List.of()
            : List.copyOf(standaloneExtensionStatements);
  }

  public List<JdbcStatement> getStatementsForType(Type type) {
    return statements.stream().filter(s -> s.getType() == type).collect(Collectors.toList());
  }

  @JsonIgnore
  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    var artifacts = new ArrayList<DeploymentArtifact>();
    artifacts.add(new DeploymentArtifact("-schema.sql", buildSchemaContent()));
    artifacts.add(new DeploymentArtifact("-views.sql", toSql(getStatementsForType(Type.VIEW))));

    standaloneExtensionStatements.stream()
        .map(stmt -> new DeploymentArtifact(formatSuffix(stmt.getName()), toSql(stmt)))
        .forEach(artifacts::add);

    return List.copyOf(artifacts);
  }

  private String buildSchemaContent() {
    return Stream.of(Type.EXTENSION, Type.TABLE, Type.INDEX)
        .map(this::getStatementsForType)
        .filter(Predicate.not(List::isEmpty))
        .map(JdbcPlan::toSql)
        .collect(Collectors.joining(";\n\n"));
  }

  private static String toSql(List<JdbcStatement> statements) {
    return DeploymentArtifact.toSqlString(statements.stream().map(JdbcStatement::getSql));
  }

  private static String toSql(JdbcStatement stmt) {
    return DeploymentArtifact.toSqlString(stmt.getSql());
  }

  private static String formatSuffix(String name) {
    return "-" + name + ".sql";
  }
}
