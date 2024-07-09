/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineFeature.STANDARD_DATABASE;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLFactory;
import com.datasqrl.engine.database.relational.ddl.JdbcDDLServiceLoader;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.DowncastFunction;
import com.datasqrl.functions.json.RowToJsonDowncastFunction;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.PhysicalDAGPlan.DatabaseStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.sql.PgExtension;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.type.JdbcTypeSerializer;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

/**
 * Abstract implementation of a JDBC-compatible database engine.
 */
@Slf4j
public abstract class AbstractJDBCDatabaseEngine extends AbstractJDBCEngine implements DatabaseEngine {


  @Getter
  final EngineConfig connectorConfig;
  private final ConnectorFactoryFactory connectorFactory;

  public AbstractJDBCDatabaseEngine(String name, @NonNull EngineConfig connectorConfig, ConnectorFactoryFactory connectorFactory) {
    super(name, Type.DATABASE, STANDARD_DATABASE);
    this.connectorConfig = connectorConfig;
    this.connectorFactory = connectorFactory;
  }

  @Override
  public boolean supportsQueryEngine(QueryEngine queryEngine) {
    return false;
  }

  @Override
  public void addQueryEngine(QueryEngine queryEngine) {
    throw new UnsupportedOperationException("JDBC database engines do not support query engines");
  }

  @Override
  public boolean supports(FunctionDefinition function) {
    //TODO: @Daniel: change to determining which functions are supported by dialect & database type
    //This is a hack - we just check that it's not a tumble window function
    return FunctionUtil.getSqrlTimeTumbleFunction(function).isEmpty();
  }

  @Override
  public TableConfig getSinkConfig(String tableName) {
    return connectorFactory
        .create(Type.DATABASE, getDialect().getId())
        .orElseThrow(()-> new RuntimeException("Could not obtain sink for dialect: " + getDialect()))
        .createSourceAndSink(
            new ConnectorFactoryContext(tableName, Map.of("table-name", tableName)));
  }

  @Override
  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(getDialect());
  }

}