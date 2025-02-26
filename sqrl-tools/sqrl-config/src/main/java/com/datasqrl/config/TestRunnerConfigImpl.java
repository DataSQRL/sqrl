package com.datasqrl.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TestRunnerConfigImpl implements TestRunnerConfiguration {

  SqrlConfig sqrlConfig;

  public Optional<Duration> getDelaySec() {
    return sqrlConfig
        .asLong("delay-sec")
        .getOptional()
        .map(sec -> Duration.of(sec, ChronoUnit.SECONDS));
  }

  public Optional<Integer> getRequiredCheckpoints() {
    return sqrlConfig.asInt("required-checkpoints").getOptional();
  }
}
