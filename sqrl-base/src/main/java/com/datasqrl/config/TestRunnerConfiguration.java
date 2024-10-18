package com.datasqrl.config;

import java.time.Duration;
import java.util.Optional;

public interface TestRunnerConfiguration {

  Optional<Duration> getDelaySec();
  Optional<Integer> getRequiredCheckpoints();

}
