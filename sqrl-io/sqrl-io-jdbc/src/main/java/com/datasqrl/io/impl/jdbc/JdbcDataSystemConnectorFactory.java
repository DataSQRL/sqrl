package com.datasqrl.io.impl.jdbc;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(DataSystemConnectorFactory.class)
public class JdbcDataSystemConnectorFactory implements DataSystemConnectorFactory {

  public static final String SYSTEM_NAME = "jdbc";

  @Override
  public String getSystemName() {
    return SYSTEM_NAME;
  }

  public JdbcDataSystemConnector getConnector(@NonNull SqrlConfig connectorConfig) {
    return connectorConfig.allAs(JdbcDataSystemConnector.class).get();
  }

  @Override
  public DataSystemConnectorSettings getSettings(@NonNull SqrlConfig connectorConfig) {
    return DataSystemConnectorSettings.builder().hasSourceTimestamp(false).build();

  }
}
