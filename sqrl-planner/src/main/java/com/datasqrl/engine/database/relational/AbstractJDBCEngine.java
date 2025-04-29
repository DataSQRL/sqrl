/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabasePhysicalPlanOld;
import com.datasqrl.engine.database.DatabaseViewPhysicalPlan.DatabaseView;
import com.datasqrl.engine.database.DatabaseViewPhysicalPlan.DatabaseViewImpl;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import com.google.common.base.Preconditions;

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