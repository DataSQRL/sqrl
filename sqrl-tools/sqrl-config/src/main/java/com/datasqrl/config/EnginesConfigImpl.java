package com.datasqrl.config;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EnginesConfigImpl implements PackageJson.EnginesConfig {
  private SqrlConfig sqrlConfig;

  public int getVersion() {
    return sqrlConfig.getVersion();
  }

  public int size() {
    return 0;
  }

  public PackageJson.EngineConfig getEngineConfig(String engineId) {
    SqrlConfig subConfig = sqrlConfig.getSubConfig(engineId);
    return new EngineConfigImpl(subConfig);
  }
}
