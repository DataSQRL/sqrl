package com.datasqrl.config;

import com.datasqrl.config.PackageJson.TestConfig;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TestConfigImpl implements TestConfig {

  private final SqrlConfig test;

  @Override
  public int getDurationSec() {
    return test.asInt("durationSec").getOptional()
        .orElse(40);
  }

  @Override
  public int getDelaySec() {
    return test.asInt("durationSec").getOptional()
        .orElse(15);
  }
}
