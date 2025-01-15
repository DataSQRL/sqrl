package com.datasqrl.config;

import com.datasqrl.config.PackageJson.EngineConfig;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EnginesConfigImpl implements PackageJson.EnginesConfig {
  private SqrlConfig sqrlConfig;

  public int getVersion() {
    return sqrlConfig.getVersion();
  }

  public Optional<EngineConfig> getEngineConfig(String engineId) {
    if (!sqrlConfig.hasSubConfig(engineId)) {
      return Optional.empty();
    }
    SqrlConfig subConfig = sqrlConfig.getSubConfig(engineId);
    return Optional.of(new EngineConfigImpl(subConfig));
  }

  @Override
  public EngineConfig getEngineConfigOrErr(String engineId) {
    sqrlConfig.validateSubConfig(engineId);
    return new EngineConfigImpl(sqrlConfig.getSubConfig(engineId));
  }
}
