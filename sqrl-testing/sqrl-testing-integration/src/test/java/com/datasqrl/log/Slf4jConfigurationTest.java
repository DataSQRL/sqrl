/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.log;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Slf4jConfigurationTest {

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
