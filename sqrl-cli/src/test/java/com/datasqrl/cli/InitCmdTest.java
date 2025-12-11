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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.cli.InitCmd.ProjectType;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class InitCmdTest {

  private static final String PROJECT_NAME = "dummy-project";

  @TempDir private Path tempDir;

  private InitCmd initCmd;

  @BeforeEach
  void setup() {
    initCmd = new InitCmd();
    initCmd.projectName = PROJECT_NAME;
  }

  @Test
  void givenExistingDirectory_whenInitProject_thenThrowsException() {
    // Given
    initCmd.projectType = ProjectType.STREAM;

    // Initialize once
    initCmd.initProject(() -> tempDir);

    // When & Then - try to initialize again in same directory
    assertThatThrownBy(() -> initCmd.initProject(() -> tempDir))
        .isInstanceOf(FileAlreadyExistsException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void givenBatchFlag_whenInitProject_thenCreatesFilesWithCorrectExecMode(boolean batch)
      throws IOException {
    var expectedMode = batch ? "BATCH" : "STREAMING";

    // Given
    initCmd.batch = batch;

    // When
    initCmd.initProject(() -> tempDir);

    // Then
    var prodPkg = tempDir.resolve(PROJECT_NAME + "-prod-package.json");
    assertThat(prodPkg).exists();

    var prodContent = Files.readString(prodPkg);
    assertThat(prodContent).contains("\"execution.runtime-mode\": \"%s\"".formatted(expectedMode));
  }

  @ParameterizedTest
  @EnumSource(ProjectType.class)
  void givenProjectType_whenInitProject_thenCreatesFilesWithCorrectVariables(ProjectType type)
      throws IOException {
    // Given
    initCmd.projectType = type;

    // When
    initCmd.initProject(() -> tempDir);

    // Then
    var prodPkg = tempDir.resolve(PROJECT_NAME + "-prod-package.json");
    var testPkg = tempDir.resolve(PROJECT_NAME + "-test-package.json");
    assertThat(prodPkg).exists();
    assertThat(testPkg).exists();

    var prodContent = Files.readString(prodPkg);
    var testContent = Files.readString(testPkg);

    // Verify project name is correctly substituted in package JSON files
    assertThat(prodContent).contains("\"main\": \"%s.sqrl\"".formatted(PROJECT_NAME));
    assertThat(testContent).contains("\"main\": \"%s.sqrl\"".formatted(PROJECT_NAME));

    // Verify type-specific expectations
    switch (type) {
      case STREAM:
        assertThat(prodContent).contains("\"flink\", \"kafka\"");
        assertThat(testContent).contains("\"flink\", \"kafka\", \"postgres\", \"vertx\"");
        assertThat(testContent).contains("\"required-checkpoints\": 1");
        break;
      case DATASET:
        assertThat(prodContent).contains("\"flink\", \"iceberg\", \"duckdb\"");
        assertThat(testContent).contains("\"flink\", \"iceberg\", \"duckdb\", \"vertx\"");
        assertThat(testContent).contains("\"required-checkpoints\": 2");
        break;
      case API:
        assertThat(prodContent).contains("\"flink\", \"postgres\", \"kafka\", \"vertx\"");
        assertThat(testContent).contains("\"flink\", \"postgres\", \"kafka\", \"vertx\"");
        assertThat(testContent).contains("\"required-checkpoints\": 1");
        break;
    }
  }
}
