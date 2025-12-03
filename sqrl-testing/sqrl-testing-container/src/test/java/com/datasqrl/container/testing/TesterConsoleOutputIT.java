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
package com.datasqrl.container.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies warnings and errors during test execution are properly surfaced in
 * the console output. This test uses a scenario with {@code "compiler": {"logger": "print"}} and a
 * nullable timestamp column used as watermark, which produces compiler warnings and causes test
 * failures due to empty results. The test verifies that important diagnostic information (warnings,
 * test reports, failure details) is visible to help users debug issues.
 */
@Slf4j
public class TesterConsoleOutputIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "null-timestamp";
  }

  @Test
  @SneakyThrows
  void givenNullTimestampAndPrintLogger_whenTestExecuted_thenErrorIsCapturedAndDisplayed() {
    ContainerError exception =
        (ContainerError)
            assertThatThrownBy(() -> sqrlCmd(testDir, "test package.json".split(" ")))
                .isInstanceOf(ContainerError.class)
                .hasMessageContaining("SQRL compilation failed")
                .actual();

    var logs = exception.getLogs();
    log.info("Container logs:\n{}", logs);

    assertThat(logs)
        .as("Logs should contain 'Test Reports' section when tests fail")
        .contains("Test Reports");

    assertThat(logs)
        .as("Logs should contain compiler warning about nullable rowtime")
        .contains("[WARN]");
  }
}
