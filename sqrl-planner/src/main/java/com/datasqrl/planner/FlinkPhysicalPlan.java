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
import com.datasqrl.planner.tables.FlinkConnectorConfig;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
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
import org.apache.flink.configuration.PipelineOptions;
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

  private static final List<String> ICEBERG_SERIALIZATION_CONFIG =
      List.of(
          "org.apache.iceberg.SerializableByteBufferMap: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}",
          "org.apache.iceberg.GenericDataFile: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}",
          "org.apache.iceberg.io.WriteResult: {type: kryo, kryo-type: registered, class: com.esotericsoftware.kryo.serializers.JavaSerializer}");

  List<String> flinkSql;
  Set<String> connectors;
  Set<String> formats;
  Set<String> functions;
  @JsonIgnore Optional<String> compiledPlan;
  @JsonIgnore Optional<String> explainedPlan;
  @JsonIgnore List<String> flinkSqlNoFunctions;
  @JsonIgnore Configuration config;

  @Override
  public List<DeploymentArtifact> getDeploymentArtifacts() {
    var builder = ImmutableList.<DeploymentArtifact>builder();
    builder.add(
        new DeploymentArtifact("-config.yaml", DeploymentArtifact.toYamlString(config)),
        new DeploymentArtifact("-sql.sql", DeploymentArtifact.toSqlString(flinkSql)),
        new DeploymentArtifact(
            "-sql-no-functions.sql", DeploymentArtifact.toSqlString(flinkSqlNoFunctions)),
        new DeploymentArtifact("-functions.sql", DeploymentArtifact.toSqlString(functions)));

    compiledPlan.map(plan -> builder.add(new DeploymentArtifact("-compiled-plan.json", plan)));
    explainedPlan.map(plan -> builder.add(new DeploymentArtifact("-explained-plan.txt", plan)));

    return builder.build();
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

    private final boolean addIcebergSerializationConfig;
    private Configuration config;

    public Builder(Configuration config, boolean addIcebergSerializationConfig) {
      this.addIcebergSerializationConfig = addIcebergSerializationConfig;
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
      statementSets.add(new ArrayList<>());
    }

    public void add(SqlNode sqlNode, Sqrl2FlinkSQLTranslator sqrlEnv) {
      add(sqlNode, sqrlEnv.toSqlString(sqlNode));
    }

    public void addFullyResolvedFunction(String createFunction) {
      fullyResolvedFunctions.add(createFunction);
    }

    public void add(List<? extends SqlNode> nodes, List<String> nodeSqls) {
      checkArgument(nodeSqls.size() == nodes.size(), "Node and SQL size mismatch during planning");

      for (int i = 0; i < nodes.size(); i++) {
        add(nodes.get(i), nodeSqls.get(i));
      }
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

      if (connectors.contains(IcebergEngineFactory.ENGINE_NAME) && addIcebergSerializationConfig) {
        // We need to enforce the Kryo JavaSerializer for some built-in Iceberg classes
        var updatedSerConf =
            ImmutableList.<String>builder()
                .addAll(config.get(PipelineOptions.SERIALIZATION_CONFIG, new ArrayList<>()))
                .addAll(ICEBERG_SERIALIZATION_CONFIG)
                .build();

        config.set(PipelineOptions.SERIALIZATION_CONFIG, updatedSerConf);
      }

      return new FlinkPhysicalPlan(
          flinkSql,
          connectors,
          formats,
          fullyResolvedFunctions,
          compiledPlan.map(CompiledPlan::asJsonString),
          explainedPlan,
          flinkSqlNoFunctions,
          config);
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
