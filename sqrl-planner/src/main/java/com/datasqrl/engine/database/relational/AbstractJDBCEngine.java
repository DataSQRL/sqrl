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

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;

/**
 * This is an abstract engine implementation that provides shared functionalities for
 * relational/jdbc-compatible engines.
 *
 * <p>It implements the physical plan construction by creating DDL statements and queries for the
 * relational database dialect.
 */
@Slf4j
public abstract class AbstractJDBCEngine extends ExecutionEngine.Base implements ExecutionEngine {

  @Getter protected final EngineConfig engineConfig;
  protected final Optional<ConnectorConf> connector;

  public AbstractJDBCEngine(
      @NonNull String name,
      @NonNull EngineType type,
      @NonNull EnumSet<EngineFeature> capabilities,
      @NonNull EngineConfig engineConfig,
      @NonNull ConnectorFactoryFactory connectorFactory) {
    super(name, type, capabilities);
    this.engineConfig = engineConfig;
    this.connector = connectorFactory.getOptionalConfig(getName());
  }

  public abstract JdbcStatementFactory getStatementFactory();

  protected abstract JdbcDialect getDialect();

  protected abstract DatabaseType getDatabaseType();

  protected abstract String getConnectorTableName(FlinkTableBuilder tableBuilder);

  public EngineCreateTable createTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis) {

    var connectorOptions = getConnectorOptions(originalTableName, tableBuilder.getTableName());
    tableBuilder.setConnectorOptions(connectorOptions);

    return new JdbcEngineCreateTable(
        getConnectorTableName(tableBuilder), tableBuilder, relDataType, tableAnalysis.get());
  }

  public EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    var stmtFactory = getStatementFactory();
    var planBuilder = JdbcPhysicalPlan.builder();
    planBuilder.stage(stagePlan.getStage());
    // Create extensions
    planBuilder.statements(stmtFactory.extractExtensions(stagePlan.getQueries()));
    // Create tables
    Set<String> tableNames = new HashSet<>();
    var jdbcCreateTables =
        stagePlan.getTables().stream().map(JdbcEngineCreateTable.class::cast).toList();
    jdbcCreateTables.stream()
        .map(stmtFactory::createTable)
        .forEach(
            stmt -> {
              planBuilder.statement(stmt);
              tableNames.add(stmt.getName());
            });
    Map<String, JdbcEngineCreateTable> tableIdMap =
        jdbcCreateTables.stream()
            .collect(
                Collectors.toMap(createTable -> createTable.getTable().getTableName(), f -> f));
    planBuilder.tableIdMap(tableIdMap);
    // Create executable queries & views
    if (stmtFactory.supportsQueries()) {
      stagePlan
          .getQueries()
          .forEach(
              query -> {
                planBuilder.query(query.getRelNode());
                var function = query.getFunction();
                var result = stmtFactory.createQuery(query, !function.hasParameters(), tableIdMap);
                if (result.getExecQueryBuilder() != null && function.getExecutableQuery() == null) {
                  function.setExecutableQuery(
                      result
                          .getExecQueryBuilder()
                          .database(getDatabaseType())
                          .stage(stagePlan.getStage())
                          .build());
                }
                // Only add views that don't clash with tables
                if (result.getView() != null && !tableNames.contains(result.getView().getName())) {
                  planBuilder.statement(result.getView());
                }
              });
    }
    return planBuilder.build();
  }

  protected Map<String, String> getConnectorOptions(String originalTableName, String tableId) {
    return connector
        .map(
            connConf ->
                connConf.toMapWithSubstitution(
                    Context.builder().tableName(originalTableName).tableId(tableId).build()))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Missing Flink connector configuration for engine: " + getName()));
  }
}
