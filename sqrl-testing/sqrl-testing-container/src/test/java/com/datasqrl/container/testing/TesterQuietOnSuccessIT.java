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

import java.nio.file.Files;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies successful test execution redirects log output to log file. This
 * test uses the logging/print test case which exports data to a print logger. The test verifies
 * that "LogData" appears in the log file and NOT in console output. Also verifies that "Captured
 * Errors" and "Test Reports" sections are NOT shown when tests succeed.
 */
@Slf4j
public class TesterQuietOnSuccessIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "logging/print";
  }

  @Test
  @SneakyThrows
  void givenPrintLogger_whenTestSucceeds_thenLogDataInFileNotConsole() {
    var result = sqrlCmd(testDir, "test package.json".split(" "));

    var consoleLogs = result.logs();
    log.info("Container logs:\n{}", consoleLogs);

    assertThat(consoleLogs)
        .as("Console should indicate successful build")
        .contains("BUILD SUCCESS");

    assertThat(consoleLogs)
        .as("Console should NOT contain 'Captured Errors' section on success")
        .doesNotContain("Captured Errors");

    assertThat(consoleLogs)
        .as("Console should NOT contain 'Test Reports' section on success")
        .doesNotContain("Test Reports");

    var logFile = testDir.resolve("build/logs/test-execution.log");
    assertThat(logFile).as("Log file should exist").exists();

    var logFileContent = Files.readString(logFile);
    log.info("Log file content length: {} bytes", logFileContent.length());

    assertThat(logFileContent)
        .as("Log file should contain LogData output from print logger")
        .contains("LogData");

    assertThat(consoleLogs)
        .as("Console should NOT contain LogData output (redirected to log file)")
        .doesNotContain("LogData");
  }
}
