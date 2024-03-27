package com.datasqrl.engine.stream.flink;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.DynamicSinkConnectorFactory;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.io.tables.FlinkConnectorFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(DynamicSinkConnectorFactory.class)
public class PrintFlinkDynamicSinkConnectorFactory extends FlinkConnectorFactory implements DynamicSinkConnectorFactory {

  public static final String CONNECTOR_TYPE = "print";
  public static final String PRINT_IDENTIFIER_KEY = "print-identifier";

  @Override
  public ConnectorConfig forName(@NonNull Name name, @NonNull SqrlConfig baseConnectorConfig) {
    SqrlConfig connector = SqrlConfig.createCurrentVersion();
    connector.setProperty(CONNECTOR_KEY, getType());
    connector.setProperty(PRINT_IDENTIFIER_KEY, name.getDisplay());
    return new ConnectorConfig(connector, this);
  }

  @Override
  public String getType() {
    return CONNECTOR_TYPE;
  }
}
