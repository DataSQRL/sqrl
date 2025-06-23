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
package com.datasqrl.planner;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.planner.tables.FlinkConnectorConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;

/**
 * Represents the physical plan for Flink as both FlinkSQL and as a compiled plan. For the FlinkSQL
 * representation we also keep track of a version without functions.
 *
 * <p>In addition, we extract all the functions, connectors, and formats for additional
 * post-compilation analysis (e.g. to determine what dependencies are needed).
 */
@Value
@Builder
public class FlinkPhysicalPlan implements EnginePhysicalPlan {

  List<String> flinkSql;
  Set<String> connectors;
  Set<String> formats;
  Set<String> functions;
  @JsonIgnore Optional<String> compiledPlan;
  @JsonIgnore Optional<String> explainedPlan;
  @JsonIgnore List<String> flinkSqlNoFunctions;

  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    List<DeploymentArtifact> deploymentArtifacts = new ArrayList<>();
    deploymentArtifacts.add(
        new DeploymentArtifact("-sql.sql", DeploymentArtifact.toSqlString(flinkSql)));
    deploymentArtifacts.add(
        new DeploymentArtifact(
            "-sql-no-functions.sql", DeploymentArtifact.toSqlString(flinkSqlNoFunctions)));
    deploymentArtifacts.add(
        new DeploymentArtifact("-functions.sql", DeploymentArtifact.toSqlString(functions)));
    compiledPlan.map(
        plan -> deploymentArtifacts.add(new DeploymentArtifact("-compiled-plan.json", plan)));
    explainedPlan.map(
        plan -> deploymentArtifacts.add(new DeploymentArtifact("-explained-plan.txt", plan)));
    return deploymentArtifacts;
  }

  @Value
  public static class Builder {
    List<String> flinkSql = new ArrayList<>();
    List<String> flinkSqlNoFunctions = new ArrayList<>();
    List<SqlNode> nodes = new ArrayList<>();
    Set<String> connectors = new HashSet<>();
    Set<String> formats = new HashSet<>();
    Set<String> fullyResolvedFunctions = new HashSet<>();
    List<RichSqlInsert> statementSet = new ArrayList<>();

    public void addInsert(RichSqlInsert insert) {
      statementSet.add(insert);
    }

    public void add(SqlNode sqlNode, Sqrl2FlinkSQLTranslator sqrlEnv) {
      add(sqlNode, sqrlEnv.toSqlString(sqlNode));
    }

    public void addFullyResolvedFunction(String createFunction) {
      fullyResolvedFunctions.add(createFunction);
    }

    public void add(SqlNode node, String nodeSql) {
      flinkSql.add(nodeSql);
      nodes.add(node);
      if (node instanceof SqlCreateTable table) {
        for (SqlNode option : table.getPropertyList().getList()) {
          var sqlTableOption = (SqlTableOption) option;
          if (sqlTableOption.getKeyString().equalsIgnoreCase(FlinkConnectorConfig.CONNECTOR_KEY)) {
            connectors.add(sqlTableOption.getValueString());
          }
          switch (sqlTableOption.getKeyString()) {
            case FlinkConnectorConfig.FORMAT_KEY:
            case FlinkConnectorConfig.KEY_FORMAT_KEY:
            case FlinkConnectorConfig.VALUE_FORMAT_KEY:
              formats.add(sqlTableOption.getValueString());
          }
        }
      }
      if (!(node instanceof SqlCreateFunction)) {
        flinkSqlNoFunctions.add(nodeSql);
      }
    }

    public SqlExecute getExecuteStatement() {
      Preconditions.checkArgument(
          !statementSet.isEmpty(), "SQRL script does not contain any sink definitions");
      var sqlStatementSet = new SqlStatementSet(statementSet, SqlParserPos.ZERO);
      return new SqlExecute(sqlStatementSet, SqlParserPos.ZERO);
    }

    public FlinkPhysicalPlan build(Optional<CompiledPlan> compiledPlan) {
      var explainedPlan =
          compiledPlan.map(
              plan ->
                  plan.explain(
                      ExplainFormat.TEXT, ExplainDetail.CHANGELOG_MODE, ExplainDetail.PLAN_ADVICE));
      return new FlinkPhysicalPlan(
          flinkSql,
          connectors,
          formats,
          fullyResolvedFunctions,
          compiledPlan.map(CompiledPlan::asJsonString),
          explainedPlan,
          flinkSqlNoFunctions);
    }
  }
}
