/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineFeature.STANDARD_DATABASE;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan;
import com.datasqrl.v2.tables.FlinkTableBuilder;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Abstract implementation of a JDBC-compatible database engine.
 */
@Slf4j
public abstract class AbstractJDBCDatabaseEngine extends AbstractJDBCEngine implements DatabaseEngine {

  public AbstractJDBCDatabaseEngine(String name, @NonNull EngineConfig engineConfig, ConnectorFactoryFactory connectorFactory) {
    super(name, EngineType.DATABASE, STANDARD_DATABASE, engineConfig, connectorFactory);
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

  //  @Override
//  public boolean supports(FunctionDefinition function) {
//    //TODO: @Daniel: change to determining which functions are supported by dialect & database type
//    //This is a hack - we just check that it's not a tumble window function
//    return FunctionUtil.getSqrlTimeTumbleFunction(function).isEmpty();
//  }

  @Deprecated
  private final ConnectorFactoryFactory connectorFactory;

  @Override
  @Deprecated
  public TableConfig getSinkConfig(String tableName) {
    return connectorFactory
        .create(EngineType.DATABASE, getDialect().getId())
        .orElseThrow(()-> new RuntimeException("Could not obtain sink for dialect: " + getDialect()))
        .createSourceAndSink(
            new ConnectorFactoryContext(tableName, Map.of("table-name", tableName)));
  }

  @Override
  @Deprecated
  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(getDialect());
  }

}