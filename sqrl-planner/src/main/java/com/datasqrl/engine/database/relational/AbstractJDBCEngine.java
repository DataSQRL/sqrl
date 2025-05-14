/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;

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
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import com.datasqrl.v2.tables.FlinkTableBuilder;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * This is an abstract engine implementation that provides shared functionalities for relational/jdbc-compatible
 * engines.
 *
 * It implements the physical plan construction by creating DDL statements and queries for the relational database dialect.
 */
@Slf4j
public abstract class AbstractJDBCEngine extends ExecutionEngine.Base implements ExecutionEngine {

  @Getter
  protected final EngineConfig engineConfig;
  protected final Optional<ConnectorConf> connector;

  public AbstractJDBCEngine(@NonNull String name, @NonNull EngineType type,
      @NonNull EnumSet<EngineFeature> capabilities,
      @NonNull EngineConfig engineConfig, @NonNull ConnectorFactoryFactory connectorFactory) {
    super(name, type, capabilities);
    this.engineConfig = engineConfig;
    this.connector = connectorFactory.getOptionalConfig(getName());
  }

  protected abstract JdbcDialect getDialect();

  protected abstract DatabaseType getDatabaseType();

  public EngineCreateTable createTable(ExecutionStage stage, String originalTableName,
      FlinkTableBuilder tableBuilder, RelDataType relDataType, Optional<TableAnalysis> tableAnalysis) {
    var tableName = tableBuilder.getTableName();
    var connect = connector.orElseThrow(() -> new IllegalArgumentException("Missing Flink connector configuration for engine: " + getName()));
    tableBuilder.setConnectorOptions(connect.toMapWithSubstitution(Context.builder()
        .tableName(tableName).origTableName(originalTableName).build()));
    return new JdbcEngineCreateTable(tableBuilder, relDataType, tableAnalysis.get());
  }

  public abstract JdbcStatementFactory getStatementFactory();

  public EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    var stmtFactory = getStatementFactory();
    var planBuilder = JdbcPhysicalPlan.builder();
    planBuilder.stage(stagePlan.getStage());
    //Create extensions
    planBuilder.statements(stmtFactory.extractExtensions(stagePlan.getQueries()));
    //Create tables
    stagePlan.getTables().stream().map(JdbcEngineCreateTable.class::cast)
        .map(stmtFactory::createTable).forEach(planBuilder::statement);
    Map<String, TableAnalysis> tableMap =
    stagePlan.getTables().stream().map(JdbcEngineCreateTable.class::cast)
        .collect(Collectors.toMap(createTable -> createTable.getTable().getTableName(),
            JdbcEngineCreateTable::getTableAnalysis));
    planBuilder.tableMap(tableMap);
    //Create executable queries & views
    if (stmtFactory.supportsQueries()) {
      stagePlan.getQueries().forEach(query -> {
        planBuilder.query(query.getRelNode());
        var function = query.getFunction();
        var result = stmtFactory.createQuery(query, !function.hasParameters());
        if (result.getExecQueryBuilder()!=null && function.getExecutableQuery()==null) {
          function.setExecutableQuery(result.getExecQueryBuilder()
              .database(getDatabaseType())
              .stage(stagePlan.getStage()).build());
        }
        if (result.getView()!=null) {
          planBuilder.statement(result.getView());
        }
      });
    }
    return planBuilder.build();
  }

}