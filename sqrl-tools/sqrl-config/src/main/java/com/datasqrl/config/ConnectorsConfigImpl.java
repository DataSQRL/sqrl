package com.datasqrl.config;

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
    return Optional.of(new ConnectorConfImpl(sqrlConfig.getSubConfig(name)));
  }
}
