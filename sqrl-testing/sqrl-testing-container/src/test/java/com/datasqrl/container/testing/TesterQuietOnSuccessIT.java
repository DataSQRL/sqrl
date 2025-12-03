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

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies successful test execution does NOT print captured errors or
 * excessive log output. This test uses the sensors-mutation test case which is known to work
 * correctly. The test verifies that the "Captured Errors" section is NOT shown when tests succeed.
 */
@Slf4j
public class TesterQuietOnSuccessIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "sensors-mutation";
  }

  @Test
  @SneakyThrows
  void givenSuccessfulTest_whenTestExecuted_thenNoErrorsDisplayed() {
    var result = sqrlCmd(testDir, "test package.json".split(" "));

    var logs = result.logs();
    log.info("Container logs:\n{}", logs);

    assertThat(logs).as("Logs should indicate successful build").contains("BUILD SUCCESS");

    assertThat(logs)
        .as("Logs should NOT contain 'Captured Errors' section on success")
        .doesNotContain("Captured Errors");

    assertThat(logs)
        .as("Logs should NOT contain 'Test Reports' section on success")
        .doesNotContain("Test Reports");
  }
}
