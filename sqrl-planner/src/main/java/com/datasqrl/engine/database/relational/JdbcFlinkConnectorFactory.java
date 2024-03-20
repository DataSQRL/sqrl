package com.datasqrl.engine.database.relational;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import lombok.NonNull;

public class JdbcFlinkConnectorFactory extends FlinkConnectorFactory implements JdbcConnectorFactory {

  public static final JdbcFlinkConnectorFactory INSTANCE = new JdbcFlinkConnectorFactory();

  private JdbcFlinkConnectorFactory() {
  }

  @Override
  public ConnectorConfig fromBaseConfig(@NonNull SqrlConfig connectorConfig, @NonNull String databaseName) {
    SqrlConfig config = SqrlConfig.create(connectorConfig);
    config.copy(connectorConfig);
    config.setProperty("table-name", databaseName);
    config.setProperty(CONNECTOR_KEY, "jdbc-sqrl");
    return new ConnectorConfig(config, this);
  }
}
