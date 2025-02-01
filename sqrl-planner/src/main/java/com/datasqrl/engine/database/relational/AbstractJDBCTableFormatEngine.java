package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineFeature.STANDARD_TABLE_FORMAT;

import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.database.AnalyticDatabaseEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;

/**
 * Abstract implementation of a relational table format database engine.
 * A table format database only persists data and does not provide an integrated
 * query engine like implementations of {@link AbstractJDBCDatabaseEngine} do.
 *
 * Hence, a compatible {@link QueryEngine} must be registered with implementations of this class
 * for query execution.
 *
 * The {@link com.datasqrl.engine.EnginePhysicalPlan} produced by a table format database has two
 * components:
 * 1) The DDL statement for the Iceberg table that is created and the corresponding catalog registration.
 * 2) An {@link com.datasqrl.engine.EnginePhysicalPlan} for each registered {@link QueryEngine} which contains
 * a) the DDL for importing the Iceberg table from the catalog and b) the queries translated to that engine.
 */
public abstract class AbstractJDBCTableFormatEngine extends AbstractJDBCEngine implements
    DatabaseEngine, AnalyticDatabaseEngine {

  @Getter
  final EngineConfig connectorConfig;
  final ConnectorFactoryFactory connectorFactory;
  final Map<String, QueryEngine> queryEngines = new LinkedHashMap<>();

  public AbstractJDBCTableFormatEngine(String name, @NonNull EngineConfig connectorConfig, ConnectorFactoryFactory connectorFactory) {
    super(name, EngineType.DATABASE, STANDARD_TABLE_FORMAT);
    this.connectorConfig = connectorConfig;
    this.connectorFactory = connectorFactory;
  }

  @Override
  public void addQueryEngine(QueryEngine queryEngine) {
    if (!supportsQueryEngine(queryEngine)) throw new UnsupportedOperationException(getName() + " table format does not support query engine: " + queryEngine);
    Preconditions.checkState(!queryEngines.containsKey(queryEngine.getName()), "Query engine already added: %s", queryEngine.getName());
    queryEngines.put(queryEngine.getName(), queryEngine);
  }

  @Override
  public boolean supports(EngineFeature capability) {
    return super.supports(capability) ||
        queryEngines.values().stream().allMatch(queryEngine -> queryEngine.supports(capability));
  }

//  @Override
//  public boolean supports(FunctionDefinition function) {
//    return queryEngines.values().stream().allMatch(queryEngine -> queryEngine.supports(function));
//  }

  @Override
  public TableConfig getSinkConfig(String tableName) {
    return connectorFactory
        .create(EngineType.DATABASE, getDialect().getId())
        .orElseThrow(()-> new RuntimeException("Could not obtain sink for dialect: " + getDialect()))
        .createSourceAndSink(
            new ConnectorFactoryContext(tableName, Map.of("table-name", tableName)));
  }

  @Override
  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(getDialect());
  }

}
