package com.datasqrl.config;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

/**
 * Placeholder for future templated connector handling
 */
@AllArgsConstructor(onConstructor_=@Inject)
public class ConnectorFactoryFactoryImpl implements ConnectorFactoryFactory {

  PackageJson packageJson;

  private Optional<ConnectorConf> getConnectorConfig(String connectorName) {
    var engineConfig = packageJson.getEngines().getEngineConfig("flink");
    Preconditions.checkArgument(engineConfig.isPresent(), "Missing engine configuration for Flink");
    var connectors = engineConfig.get().getConnectors();
    return connectors.getConnectorConfig(connectorName);
  }

  @Override
  public ConnectorConf getConfig(String name) {
    var connectors = packageJson.getEngines().getEngineConfigOrErr("flink")
        .getConnectors();
    return connectors.getConnectorConfigOrErr(name);
  }

  @Override
  public Optional<ConnectorConf> getOptionalConfig(String name) {
    var connectors = packageJson.getEngines().getEngineConfigOrErr("flink")
        .getConnectors();
    return connectors.getConnectorConfig(name);
  }

}
