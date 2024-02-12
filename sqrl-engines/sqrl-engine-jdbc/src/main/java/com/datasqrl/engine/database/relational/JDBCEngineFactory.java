package com.datasqrl.engine.database.relational;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnectorFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class JDBCEngineFactory implements DatabaseEngineFactory {
  public static final String ENGINE_NAME = "jdbc";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public JDBCEngine initialize(@NonNull SqrlConfig config) {
    return new JDBCEngine(getConnector(config));
  }

  private JdbcDataSystemConnector getConnector(@NonNull SqrlConfig config) {
    return new JdbcDataSystemConnectorFactory().getConnector(config);
  }
}
