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
import org.junit.jupiter.api.Test;

class JBangContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "jbang";
  }

  @Test
  void givenProjectWithJBangJarExport_whenTestCommandExecuted_thenCompileAndTestSuccessful()
      throws Exception {
    var res = sqrlCmd(testDir, "test", "package.json");

    assertThat(res.logs()).contains("MyTableTest", "MyAsyncTableTest", "BUILD SUCCESS");

    var fatJar = testDir.resolve("build/deploy/flink/lib/jbang-udfs.jar");
    assertThat(fatJar).exists().isRegularFile();
    assertThat(Files.size(fatJar))
        .as("Fat JAR should only contain UDF classes and declared //DEPS, not the entire classpath")
        .isLessThan(10_000_000L);
  }
}
