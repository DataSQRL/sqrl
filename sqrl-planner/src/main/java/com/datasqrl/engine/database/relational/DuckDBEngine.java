package com.datasqrl.engine.database.relational;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.google.inject.Inject;

import lombok.NonNull;

public class DuckDBEngine extends AbstractJDBCQueryEngine {

  @Inject
  public DuckDBEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(DuckDBEngineFactory.ENGINE_NAME, json.getEngines().getEngineConfig(DuckDBEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(DuckDBEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected DatabaseType getDatabaseType() {
    return DatabaseType.DUCKDB;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new DuckDbStatementFactory(engineConfig);
  }

}
