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
package com.datasqrl.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DatasqrlRunTest {

  @TempDir private Path tempDir;

  private Configuration flinkConfig;
  private Map<String, String> env;

  private DatasqrlRun underTest;

  @BeforeEach
  void setup() {
    flinkConfig = mock(Configuration.class);
    env = new HashMap<>();

    underTest = DatasqrlRun.nonBlocking(tempDir.resolve("plan"), null, flinkConfig, env);
  }

  @Test
  void returnsEmptyWhenNoSavepointDirConfigured() {
    assertThat(underTest.getLastSavepoint()).isEmpty();
  }

  @Test
  void usesSavepointDirFromFlinkConfig() throws Exception {
    // create two savepoint directories with different creation times
    Files.createDirectory(tempDir.resolve("sp1"));
    Thread.sleep(1000);
    Path sp2 = Files.createDirectory(tempDir.resolve("sp2"));

    String uri = tempDir.toUri().toString();
    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY)).thenReturn(uri);

    var result = underTest.getLastSavepoint();

    assertThat(result).isPresent();
    assertThat(result.get()).isEqualTo(sp2.toAbsolutePath().toString());
  }

  @Test
  void returnsEmptyIfSavepointDirConfigBlank() {
    underTest = DatasqrlRun.nonBlocking(tempDir.resolve("plan"), null, flinkConfig, env);

    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY)).thenReturn(" ");

    assertThat(underTest.getLastSavepoint()).isEmpty();
  }

  @Test
  void returnsEmptyIfDirectoryDoesNotExist() {
    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY))
        .thenReturn("file:///nonexistent-dir");

    assertThat(underTest.getLastSavepoint()).isEmpty();
  }

  @Test
  void fallsBackToPathStringIfUriSyntaxInvalid() throws Exception {
    // Create one savepoint
    Files.createDirectory(tempDir.resolve("sp1"));

    // Provide an invalid URI (e.g., no scheme)
    when(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY))
        .thenReturn(tempDir.toAbsolutePath().toString());

    var result = underTest.getLastSavepoint();

    assertThat(result).isPresent();
    assertThat(result.get()).endsWith("sp1");
  }
}
