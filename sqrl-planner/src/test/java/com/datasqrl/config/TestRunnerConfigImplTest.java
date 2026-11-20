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
package com.datasqrl.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.CollectedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestRunnerConfigImplTest {

  @TempDir Path tempDir;

  private Path workspaceDir;
  private Path projectDir;
  private ProjectPaths paths;
  private SqrlConfig testRunner;

  @BeforeEach
  void setUp() throws IOException {
    workspaceDir = Files.createDirectories(tempDir.resolve("workspace"));
    projectDir = Files.createDirectories(workspaceDir.resolve("project"));
    paths = new ProjectPaths(workspaceDir, projectDir);
    testRunner = SqrlConfig.createCurrentVersion().getSubConfig("test-runner");
  }

  @Test
  void givenFoldersInsideProject_whenGetFolders_thenResolvesThemAgainstProjectRoot()
      throws IOException {
    Files.createDirectories(projectDir.resolve("api/tests"));
    testRunner.setProperty("snapshot-folder", "./snapshots/../snapshots-v1");
    testRunner.setProperty("test-folder", "api/tests/");

    var config = new TestRunnerConfigImpl(testRunner);

    assertThat(config.getSnapshotDir(paths)).isEqualTo(projectDir.resolve("snapshots-v1"));
    assertThat(config.getTestDir(paths)).contains(projectDir.resolve("api/tests"));
  }

  @Test
  void givenTestFolderOfSiblingInWorkspace_whenGetTestDir_thenResolvesIt() throws IOException {
    var sharedTests = Files.createDirectories(workspaceDir.resolve("shared/tests"));
    testRunner.setProperty("test-folder", "../shared/tests");

    var config = new TestRunnerConfigImpl(testRunner);

    assertThat(config.getTestDir(paths)).contains(sharedTests);
  }

  @ParameterizedTest
  @ValueSource(strings = {"snapshot-folder", "test-folder"})
  void givenFolderOutsideWorkspace_whenGetFolder_thenFailsNamingKeyAndValue(String key) {
    testRunner.setProperty(key, "../../outside");

    assertThatThrownBy(() -> getFolder(key))
        .isInstanceOf(CollectedException.class)
        .hasMessage(
            "Value [../../outside] for key [test-runner.%s] is not valid. %s",
            key, ProjectPaths.RULE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"snapshot-folder", "test-folder"})
  void givenAbsoluteFolder_whenGetFolder_thenFailsNamingKeyAndValue(String key) {
    var absolute = projectDir.resolve("snapshots").toAbsolutePath().toString();
    testRunner.setProperty(key, absolute);

    assertThatThrownBy(() -> getFolder(key))
        .isInstanceOf(CollectedException.class)
        .hasMessage(
            "Value [%s] for key [test-runner.%s] is not valid. %s",
            absolute, key, ProjectPaths.RULE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"snapshot-folder", "test-folder"})
  void givenFolderBelowLinkToOutsideWorkspace_whenGetFolder_thenFails(String key)
      throws IOException {
    var outside = Files.createDirectories(tempDir.resolve("outside"));
    Files.createSymbolicLink(projectDir.resolve("linked"), outside);
    testRunner.setProperty(key, "linked/folder");

    assertThatThrownBy(() -> getFolder(key))
        .isInstanceOf(CollectedException.class)
        .hasMessage(
            "Value [linked/folder] for key [test-runner.%s] is not valid. %s",
            key, ProjectPaths.RULE);
  }

  private Object getFolder(String key) {
    var config = new TestRunnerConfigImpl(testRunner);
    return key.equals("snapshot-folder") ? config.getSnapshotDir(paths) : config.getTestDir(paths);
  }
}
