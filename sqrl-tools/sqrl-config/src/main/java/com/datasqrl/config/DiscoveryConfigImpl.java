package com.datasqrl.config;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DiscoveryConfigImpl implements PackageJson.DiscoveryConfig {
  SqrlConfig sqrlConfig;

  public DataDiscoveryConfigImpl getDataDiscoveryConfig() {
    return DataDiscoveryConfigImpl.of(sqrlConfig, sqrlConfig.getErrorCollector());
  }
}
