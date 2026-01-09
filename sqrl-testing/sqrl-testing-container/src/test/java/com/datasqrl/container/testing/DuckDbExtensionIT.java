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
import org.junit.jupiter.api.Test;

public class DuckDbExtensionIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "duckdb";
  }

  @Test
  @SneakyThrows
  void givenUdfScript_whenCompiledAndServerStarted_thenApiRespondsCorrectly() {
    var res = sqrlCmd(testDir, "test", "package.json");

    // Verify the expected success messages are present in the logs
    assertThat(res.logs())
        .contains("Running Tests")
        .contains("iceberg-query")
        .contains("postgres-query")
        .contains("BUILD SUCCESS");
  }
}
