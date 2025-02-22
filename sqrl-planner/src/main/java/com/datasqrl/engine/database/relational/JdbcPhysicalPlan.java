package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.DatabasePhysicalPlan;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.v2.analyzer.TableAnalysis;
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

  @JsonIgnore
  ExecutionStage stage;
  @Singular
  List<JdbcStatement> statements;

  /**
   * Queries that are used for index selection
   * @param type
   * @return
   */
  @JsonIgnore
  @Singular
  List<RelNode> queries;
  /**
   * A mapping of CREATE TABLE from their materialized
   * name to the original TableAnalysis
   */
  @JsonIgnore
  Map<String, TableAnalysis> tableMap;

  public List<JdbcStatement> getStatementsForType(JdbcStatement.Type type) {
    return statements.stream().filter(s -> s.getType()==type).collect(Collectors.toList());
  }

  private static String toSql(List<JdbcStatement> statements) {
    return DeploymentArtifact.toSqlString(statements.stream().map(JdbcStatement::getSql));
  }

  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    return List.of(
        new DeploymentArtifact("-schema.sql",
            Stream.of(Type.EXTENSION, Type.TABLE, Type.INDEX)
                .map(this::getStatementsForType)
                .filter(Predicate.not(List::isEmpty))
                .map(JdbcPhysicalPlan::toSql)
            .collect(Collectors.joining(";\n\n"))),
        new DeploymentArtifact("-views.sql", toSql(getStatementsForType(Type.VIEW)))
    );
  }
}
