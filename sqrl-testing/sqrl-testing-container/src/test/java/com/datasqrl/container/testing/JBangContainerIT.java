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

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JBangContainerIT extends SqrlContainerTestBase {

  @BeforeEach
  @Override
  void setupBeforeEach() {
    testDir = Path.of("src/test/resources", getTestCaseName());
  }

  @Override
  protected String getTestCaseName() {
    return "jbang";
  }

  @Test
  void givenProjectWithJBangJarExport_whenTestCommandExecuted_thenCompileAndTestSuccessful() {
    var res = sqrlScript(testDir, "test", "-c", "package.json");

    assertThat(res.logs())
        .contains("Snapshot OK for MyTableTest")
        .contains("Snapshot OK for MyAsyncTableTest");
  }
}
