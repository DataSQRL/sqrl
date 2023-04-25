package com.datasqrl.io.impl.jdbc;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(DataSystemConnectorFactory.class)
public class JdbcDataSystemConnectorFactory implements DataSystemConnectorFactory {

  public static final String SYSTEM_NAME = "jdbc";

  @Override
  public String getSystemName() {
    return SYSTEM_NAME;
  }

  @Override
  public JdbcDataSystemConnector initialize(@NonNull SqrlConfig connectorConfig) {
    return connectorConfig.allAs(JdbcDataSystemConnector.class).get();
  }
}
