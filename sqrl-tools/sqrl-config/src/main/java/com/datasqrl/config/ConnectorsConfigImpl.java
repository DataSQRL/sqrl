package com.datasqrl.config;

import com.datasqrl.config.PackageJson.EngineConfig;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ConnectorsConfigImpl implements ConnectorsConfig {

  SqrlConfig sqrlConfig;

  @Override
  public Optional<ConnectorConf> getConnectorConfig(String name) {
    if (!sqrlConfig.hasSubConfig(name)) {
      return Optional.empty();
    }
    return Optional.of(
        new ConnectorConfImpl(sqrlConfig.getSubConfig(name)));
  }

  @Override
  public ConnectorConf getConnectorConfigOrErr(String name) {
    sqrlConfig.validateSubConfig(name);
    return new ConnectorConfImpl(sqrlConfig.getSubConfig(name));
  }
}
