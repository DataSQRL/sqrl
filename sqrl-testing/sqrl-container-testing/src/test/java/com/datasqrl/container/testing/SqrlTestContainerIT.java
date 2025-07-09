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
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

@Slf4j
public class SqrlTestContainerIT extends SqrlContainerTestBase {

  @Override
  protected String getTestCaseName() {
    return "avro-schema";
  }

  @Test
  @SneakyThrows
  void givenAvroSchemaScript_whenTestCommandExecuted_thenSnapshotsValidateSuccessfully() {
    var result = sqrlScript(testDir, "test avro-schema.sqrl".split(" "));

    var logs = result.logs();
    log.info("SQRL test command executed successfully");
    log.info("Container logs:\n{}", logs);

    // Verify the expected success messages are present in the logs
    assertThat(logs)
        .contains("Snapshot OK for MySchemaQuery")
        .contains("Snapshot OK for MySchema");

    // Validate log files are present and have content
	assertLogFiles(logs, testDir);

    log.info("All snapshot validations passed successfully");
  }

  @Test
  @SneakyThrows
  void givenAvroPackage_whenTestCommandExecuted_thenSnapshotsValidateSuccessfully() {
    var snapshots = testDir.resolve("snapshots");
    FileUtils.deleteDirectory(snapshots.toFile());

    // Assert that the test command throws a RuntimeException and capture the exception
    ContainerError exception =
        (ContainerError)
            assertThatThrownBy(
                    () -> sqrlScript(testDir, "test -c complete-package.json".split(" ")))
                .isInstanceOf(ContainerError.class)
                .hasMessageContaining("SQRL compilation failed")
                .actual();

    var logs = exception.getLogs();
    log.info("Container logs:\n{}", logs);

    // Assert that the logs contain the expected error messages
    assertThat(logs).contains("Snapshot created for test: MySchema");

    assertOwner(snapshots, logs);
    FileUtils.deleteDirectory(snapshots.toFile());
  }
}
