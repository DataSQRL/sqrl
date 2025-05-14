package com.datasqrl.engine.database.relational;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.iceberg.IcebergDataTypeMapper;
import com.datasqrl.engine.database.QueryEngine;
import com.google.inject.Inject;

import lombok.NonNull;

public class IcebergEngine extends AbstractJDBCTableFormatEngine {

  @Inject
  public IcebergEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(IcebergEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfig(IcebergEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(IcebergEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  public boolean supportsQueryEngine(QueryEngine engine) {
    return engine instanceof SnowflakeEngine || engine instanceof DuckDBEngine;
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Iceberg;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new IcebergStatementFactory();
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return new IcebergDataTypeMapper();
  }

}
