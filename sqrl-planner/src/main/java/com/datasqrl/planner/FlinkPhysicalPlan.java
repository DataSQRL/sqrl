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

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.relational.IcebergEngineFactory;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.planner.tables.FlinkConnectorConfigWrapper;
import com.datasqrl.planner.util.CompiledPlanCondenser;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.configuration.Configuration;
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
  @JsonIgnore Configuration config;
  @JsonIgnore ListMultimap<Integer, String> flinkSqlBatched;
  @JsonIgnore ListMultimap<Integer, String> flinkSqlNoFunctionsBatched;

  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    var builder = ImmutableList.<DeploymentArtifact>builder();
    builder.add(
        new DeploymentArtifact("-config.yaml", DeploymentArtifact.toYamlString(config)),
        new DeploymentArtifact("-sql.sql", DeploymentArtifact.toSqlString(flinkSql)),
        new DeploymentArtifact(
            "-sql-no-functions.sql", DeploymentArtifact.toSqlString(flinkSqlNoFunctions)),
        new DeploymentArtifact("-functions.sql", DeploymentArtifact.toSqlString(functions)));

    convertNestedList(flinkSqlBatched, "-sql").ifPresent(builder::addAll);
    convertNestedList(flinkSqlNoFunctionsBatched, "-sql-no-functions").ifPresent(builder::addAll);

    compiledPlan.map(plan -> builder.add(new DeploymentArtifact("-compiled-plan.json", plan)));
    compiledPlan
        .map(CompiledPlanCondenser::condense)
        .map(plan -> builder.add(new DeploymentArtifact("-compiled-plan-summary.json", plan)));
    explainedPlan.map(plan -> builder.add(new DeploymentArtifact("-explained-plan.txt", plan)));

    return builder.build();
  }

  /**
   * Converts a nested list of SQL statement batches into numbered deployment artifacts. Returns
   * empty if the list contains fewer than 2 batches, as a single batch does not require splitting.
   *
   * @param sqlBatches list of SQL statement batches to convert
   * @param suffixName suffix used to name each artifact (e.g. "-sql" produces "-sql-0.sql")
   * @return an optional list of deployment artifacts, one per batch
   */
  private static Optional<List<DeploymentArtifact>> convertNestedList(
      ListMultimap<Integer, String> sqlBatches, String suffixName) {

    var batchNum = sqlBatches.keySet().size();
    if (batchNum < 2) {
      return Optional.empty();
    }

    var res = new ArrayList<DeploymentArtifact>(batchNum);
    for (var batchIdx : sqlBatches.keys()) {
      var sql = sqlBatches.get(batchIdx);

      res.add(
          new DeploymentArtifact(
              suffixName + "-" + batchIdx + ".sql", DeploymentArtifact.toSqlString(sql)));
    }

    return Optional.of(res);
  }

  @Getter
  public static class Builder {
    private final List<String> flinkSql = new ArrayList<>();
    private final List<String> flinkSqlNoFunctions = new ArrayList<>();
    private final List<SqlNode> nodes = new ArrayList<>();
    private final Set<String> connectors = new HashSet<>();
    private final Set<String> formats = new HashSet<>();
    private final Set<String> fullyResolvedFunctions = new HashSet<>();
    private final List<List<RichSqlInsert>> statementSets = new ArrayList<>();
    private final ArrayListMultimap<Integer, String> flinkSqlBatched = ArrayListMultimap.create();
    private final ArrayListMultimap<Integer, String> flinkSqlNoFunctionsBatched =
        ArrayListMultimap.create();

    private Configuration config;

    public Builder(Configuration config) {
      this.config = config.clone();
      nextBatch();
    }

    public void addInsert(RichSqlInsert insert, @Nullable Integer batchIdx) {
      var idx = batchIdx != null ? batchIdx : statementSets.size() - 1;
      statementSets.get(idx).add(insert);
    }

    public int currentBatch() {
      return statementSets.size() - 1;
    }

    public void nextBatch() {
      var nextIdx = statementSets.size();

      statementSets.add(new ArrayList<>());

      // Replay all previous batches DDL statements
      if (nextIdx > 0) {
        flinkSqlBatched.putAll(nextIdx, flinkSqlBatched.get(nextIdx - 1));
        flinkSqlNoFunctionsBatched.putAll(nextIdx, flinkSqlNoFunctionsBatched.get(nextIdx - 1));
      }
    }

    public void add(SqlNode sqlNode) {
      add(sqlNode, RelToFlinkSql.convertToString(sqlNode), currentBatch());
    }

    public void addFullyResolvedFunction(String createFunction) {
      fullyResolvedFunctions.add(createFunction);
    }

    public void add(List<? extends SqlNode> nodes, List<String> nodeSqls) {
      checkArgument(nodeSqls.size() == nodes.size(), "Node and SQL size mismatch during planning");

      for (int i = 0; i < nodes.size(); i++) {
        add(nodes.get(i), nodeSqls.get(i), i);
      }
    }

    private void add(SqlNode node, String nodeSql, int batchIdx) {
      flinkSql.add(nodeSql);
      flinkSqlBatched.put(batchIdx, nodeSql);
      nodes.add(node);

      if (node instanceof SqlCreateTable table) {
        for (SqlNode option : table.getPropertyList().getList()) {
          var sqlTableOption = (SqlTableOption) option;
          if (sqlTableOption
              .getKeyString()
              .equalsIgnoreCase(FlinkConnectorConfigWrapper.CONNECTOR_KEY)) {
            connectors.add(sqlTableOption.getValueString());
          }
          switch (sqlTableOption.getKeyString()) {
            case FlinkConnectorConfigWrapper.FORMAT_KEY:
            case FlinkConnectorConfigWrapper.KEY_FORMAT_KEY:
            case FlinkConnectorConfigWrapper.VALUE_FORMAT_KEY:
              formats.add(sqlTableOption.getValueString());
          }
        }
      }

      if (!(node instanceof SqlCreateFunction)) {
        flinkSqlNoFunctions.add(nodeSql);
        flinkSqlNoFunctionsBatched.put(batchIdx, nodeSql);
      }
    }

    public void addInferredConfig(Configuration inferredConfig) {
      // Make sure inferred Flink config cannot override already present config
      inferredConfig.addAll(config);
      config = inferredConfig.clone();
    }

    public List<SqlExecute> getExecuteStatements() {
      checkArgument(hasSink(), "SQRL script does not contain any sink definitions");

      return statementSets.stream()
          .filter(inserts -> !inserts.isEmpty())
          .map(inserts -> new SqlStatementSet(inserts, SqlParserPos.ZERO))
          .map(ss -> new SqlExecute(ss, SqlParserPos.ZERO))
          .toList();
    }

    public FlinkPhysicalPlan build(Optional<CompiledPlan> compiledPlan) {
      var explainedPlan =
          compiledPlan.map(
              plan ->
                  plan.explain(
                      ExplainFormat.TEXT, ExplainDetail.CHANGELOG_MODE, ExplainDetail.PLAN_ADVICE));

      if (connectors.contains(IcebergEngineFactory.ENGINE_NAME)) {
        // Make sure we use the V2 sink
        config.setString("table.exec.iceberg.use-v2-sink", "true");
      }

      return new FlinkPhysicalPlan(
          flinkSql,
          connectors,
          formats,
          fullyResolvedFunctions,
          compiledPlan.map(CompiledPlan::asJsonString),
          explainedPlan,
          flinkSqlNoFunctions,
          config,
          flinkSqlBatched,
          flinkSqlNoFunctionsBatched);
    }

    private boolean hasSink() {
      for (List<RichSqlInsert> statementSet : statementSets) {
        if (!statementSet.isEmpty()) {
          return true;
        }
      }
      return false;
    }
  }
}
