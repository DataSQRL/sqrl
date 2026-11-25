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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProjectRootOptionTest {

  @TempDir Path tempDir;

  private Path workspaceDir;

  @BeforeEach
  void setUp() throws IOException {
    workspaceDir = Files.createDirectories(tempDir.resolve("workspace"));
    Files.createDirectories(workspaceDir.resolve("project"));
    Files.createDirectories(tempDir.resolve("outside"));
  }

  @Test
  void givenProjectInWorkspace_whenResolve_thenReturnsIt() {
    assertThat(ProjectRootOption.resolve(workspaceDir, Path.of("project")))
        .isEqualTo(workspaceDir.resolve("project"));
  }

  @Test
  void givenProjectOutsideWorkspace_whenResolve_thenThrows() {
    assertThatThrownBy(() -> ProjectRootOption.resolve(workspaceDir, Path.of("../outside")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Project root must be inside the workspace");
  }

  @Test
  void givenProjectBehindDotDotAfterLinkToOutside_whenResolve_thenThrows() throws IOException {
    Files.createDirectories(tempDir.resolve("outside/a"));
    Files.createDirectories(tempDir.resolve("outside/x"));
    Files.createSymbolicLink(workspaceDir.resolve("project/link"), tempDir.resolve("outside/a"));

    // Normalized, the root is project/x, but the OS resolves it to outside/x
    assertThatThrownBy(() -> ProjectRootOption.resolve(workspaceDir, Path.of("project/link/../x")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Project root must be inside the workspace");
  }

  @Test
  void givenProjectLinkedToOutsideWorkspace_whenResolve_thenThrows() throws IOException {
    Files.createSymbolicLink(workspaceDir.resolve("linked"), tempDir.resolve("outside"));

    assertThatThrownBy(() -> ProjectRootOption.resolve(workspaceDir, Path.of("linked")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Project root must be inside the workspace");
  }
}
