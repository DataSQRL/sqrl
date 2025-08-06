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
import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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

  protected abstract JdbcDialect getDialect();

  protected abstract DatabaseType getDatabaseType();

  public EngineCreateTable createTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis) {
    var tableId = tableBuilder.getTableName();
    var connect =
        connector.orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Missing Flink connector configuration for engine: " + getName()));

    if (!supports(EngineFeature.ACCESS_WITHOUT_PARTITION)) {
      var pkOpt = tableBuilder.getPrimaryKey();
      Preconditions.checkArgument(pkOpt.isPresent(), "Missing primary key: " + originalTableName);
      var partitionKey = tableBuilder.getPartition();
      for (String partitionCol : partitionKey) {
        if (!pkOpt.get().contains(partitionCol)) {
          throw new IllegalArgumentException(
              "%s engine requires that the partition key column [%s] is part of the primary key %s for table: %s"
                  .formatted(getName(), partitionCol, pkOpt.get(), originalTableName));
        }
      }
    }

    if (!supports(EngineFeature.PARTITIONED_WRITES)) {
      // Need to remove partitions from
      tableBuilder.setPartition(List.of());
    }

    tableBuilder.setConnectorOptions(
        connect.toMapWithSubstitution(
            Context.builder().tableName(originalTableName).tableId(tableId).build()));
    return new JdbcEngineCreateTable(
        getConnectorTableName(tableBuilder), tableBuilder, relDataType, tableAnalysis.get());
  }

  public abstract String getConnectorTableName(FlinkTableBuilder tableBuilder);

  public abstract JdbcStatementFactory getStatementFactory();

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
    Map<String, CreateTableJdbcStatement> tableIdMap = new HashMap<>();
    Map<String, JdbcEngineCreateTable> tableId2Create =
        jdbcCreateTables.stream()
            .collect(Collectors.toMap(tbl -> tbl.getTable().getTableName(), Function.identity()));
    jdbcCreateTables.stream()
        .map(stmtFactory::createTable)
        .forEach(
            stmt -> {
              planBuilder.statement(stmt);
              tableNames.add(stmt.getName());
              if (stmt instanceof CreateTableJdbcStatement createTbl) {
                tableIdMap.put(createTbl.getEngineTable().getTable().getTableName(), createTbl);
              }
            });
    planBuilder.tableIdMap(tableIdMap);
    // Create executable queries & views
    if (stmtFactory.supportsQueries()) {
      stagePlan
          .getQueries()
          .forEach(
              query -> {
                planBuilder.query(query.getRelNode());
                var function = query.getFunction();
                var result =
                    stmtFactory.createQuery(query, !function.hasParameters(), tableId2Create);
                if (result.getExecQueryBuilder() != null && function.getExecutableQuery() == null) {
                  function.setExecutableQuery(
                      result
                          .getExecQueryBuilder()
                          .cacheDuration(function.getCacheDuration())
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
}
