package com.datasqrl.log;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class Slf4jConfigurationTest {

  @Test
  void shouldFailIfSlf4jIsNotProperlyBound() {
    Logger logger = LoggerFactory.getLogger(Slf4jConfigurationTest.class);
    String loggerClass = logger.getClass().getName();

    System.out.println("SLF4J logger implementation: " + loggerClass);

    // SLF4J NOP Logger is the fallback when no provider is found
    assertThat(loggerClass)
        .as("SLF4J is using a no-op logger (likely due to missing or misconfigured provider)")
        .doesNotContain("NOPLogger");

    // Optional: block 1.7.x providers if you're on SLF4J 2.x
    assertThat(loggerClass)
        .as("SLF4J is using an old provider targeting 1.7.x")
        .doesNotContain("slf4j.impl.StaticLoggerBinder");
  }
}
