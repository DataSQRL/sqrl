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
import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ProjectPathsTest {

  @TempDir Path tempDir;

  private Path workspaceDir;
  private Path projectDir;
  private Path outsideDir;
  private ProjectPaths paths;

  @BeforeEach
  void setUp() throws IOException {
    workspaceDir = Files.createDirectories(tempDir.resolve("workspace"));
    projectDir = Files.createDirectories(workspaceDir.resolve("project"));
    outsideDir = Files.createDirectories(tempDir.resolve("outside"));
    paths = new ProjectPaths(workspaceDir, projectDir);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "main.sqrl",
        "./snapshots",
        "api/../schema.graphqls",
        "../shared/schema.graphqls",
        "../project/main.sqrl",
        ".."
      })
  void givenPathInsideWorkspace_whenResolve_thenResolvesAgainstProjectRoot(String path) {
    assertThat(paths.resolve("script.graphql", path, ErrorCollector.root()))
        .isEqualTo(projectDir.resolve(path).normalize());
  }

  @ParameterizedTest
  @ValueSource(strings = {"../..", "../../outside/ops.graphql", "../shared/../../notes.txt"})
  void givenPathLeavingWorkspace_whenResolve_thenFailsNamingKeyAndValue(String path) {
    assertThatThrownBy(() -> paths.resolve("script.graphql", path, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessage(
            "Value [%s] for key [script.graphql] is not valid. %s", path, ProjectPaths.RULE);
  }

  @Test
  void givenDotDotAfterLinkToOutsideWorkspace_whenResolve_thenFails() throws IOException {
    // The normalized path is the project's x, but the OS resolves it next to the link's target
    Files.createDirectories(outsideDir.resolve("a"));
    Files.createSymbolicLink(projectDir.resolve("link"), outsideDir.resolve("a"));

    assertThatThrownBy(
            () ->
                paths.resolve(
                    "script.include.lib.package", "link/../x/package.json", ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("for key [script.include.lib.package] is not valid");
  }

  @Test
  void givenDotDotAfterLinkWhoseNormalizedPathLeavesWorkspace_whenResolve_thenFails()
      throws IOException {
    // The OS resolves link/../../.. to the workspace, but the normalized path is above it
    var target = Files.createDirectories(workspaceDir.resolve("a/b/c"));
    Files.createSymbolicLink(projectDir.resolve("link"), target);

    assertThatThrownBy(
            () -> paths.resolve("script.graphql", "link/../../..", ErrorCollector.root()))
        .isInstanceOf(CollectedException.class);
  }

  @Test
  void givenDotDotAfterLinkInsideWorkspace_whenResolve_thenResolvesNormalizedPath()
      throws IOException {
    var target = Files.createDirectories(workspaceDir.resolve("shared/dir"));
    Files.createSymbolicLink(projectDir.resolve("link"), target);

    assertThat(paths.resolve("script.graphql", "link/../x.graphqls", ErrorCollector.root()))
        .isEqualTo(projectDir.resolve("x.graphqls"));
  }

  @Test
  void givenCollectorThatDoesNotAbort_whenResolvePathLeavingWorkspace_thenStillThrows() {
    var errors = ErrorCollector.root().abortOnFatal(false);

    assertThatThrownBy(() -> paths.resolve("script.main", "../../main.sqrl", errors))
        .isInstanceOf(CollectedException.class);
    assertThat(errors.hasErrors()).isTrue();
  }

  @Test
  void givenAbsolutePathInsideWorkspace_whenResolve_thenFails() {
    var absolute = projectDir.resolve("main.sqrl").toAbsolutePath().toString();

    assertThatThrownBy(() -> paths.resolve("script.main", absolute, ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("for key [script.main] is not valid");
  }

  @Test
  void givenLinkToSiblingInWorkspace_whenResolve_thenResolves() throws IOException {
    var shared = Files.createDirectories(workspaceDir.resolve("shared"));
    Files.createSymbolicLink(projectDir.resolve("shared"), shared);

    assertThat(paths.resolve("script.graphql", "shared/schema.graphqls", ErrorCollector.root()))
        .isEqualTo(projectDir.resolve("shared/schema.graphqls"));
  }

  @Test
  void givenLinkToOutsideWorkspace_whenResolve_thenFails() throws IOException {
    Files.createSymbolicLink(projectDir.resolve("snapshots"), outsideDir);

    // A folder that the test runner would create below the link counts as well
    assertThatThrownBy(
            () ->
                paths.resolve(
                    "test-runner.snapshot-folder", "snapshots/new", ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessageContaining("for key [test-runner.snapshot-folder] is not valid");
  }

  @Test
  void givenWorkspaceReachedThroughLink_whenIsInsideWorkspace_thenComparesRealPaths()
      throws IOException {
    var workspaceLink = Files.createSymbolicLink(tempDir.resolve("workspace-link"), workspaceDir);
    var linkedPaths = new ProjectPaths(workspaceLink, workspaceLink.resolve("project"));

    assertThat(linkedPaths.isInsideWorkspace(projectDir.resolve("main.sqrl"))).isTrue();
    assertThat(linkedPaths.isInsideWorkspace(outsideDir)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"main.sqrl", "./main.sqrl", "scripts/../main.sqrl", "..hidden/x.sqrl"})
  void givenPathThatStaysInside_whenStaysInside_thenTrue(String path) {
    assertThat(ProjectPaths.staysInside(path)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"..", "../main.sqrl", "scripts/../../main.sqrl", "/etc/hosts"})
  void givenPathThatLeavesOrIsAbsolute_whenStaysInside_thenFalse(String path) {
    assertThat(ProjectPaths.staysInside(path)).isFalse();
  }

  @Test
  void givenAllPathsInsideWorkspace_whenValidate_thenPasses() {
    var config = packageJson();
    config.setProperty("script.api.v1.operations", List.of("../shared/ops.graphql"));
    config.setProperty("script.api.v1.mcp-apps", "apps");
    config.setProperty("script.include.lib.package", "../lib/package.json");

    paths.validate(new PackageJsonImpl(config), ErrorCollector.root());
  }

  @ParameterizedTest
  @CsvSource({
    "script.main, false",
    "script.graphql, false",
    "script.operations, true",
    "script.database, false",
    "script.api.v1.schema, false",
    "script.api.v1.operations, true",
    "script.api.v1.openapi, false",
    "script.api.v1.mcp-apps, false",
    "script.include.lib.package, false",
    "test-runner.snapshot-folder, false",
    "test-runner.test-folder, false"
  })
  void givenPathOutsideWorkspace_whenValidate_thenFailsNamingKeyAndValue(
      String key, boolean isList) {
    var config = packageJson();
    var path = "../../outside/file";
    config.setProperty(key, isList ? List.of(path) : path);

    assertThatThrownBy(() -> paths.validate(new PackageJsonImpl(config), ErrorCollector.root()))
        .isInstanceOf(CollectedException.class)
        .hasMessage("Value [%s] for key [%s] is not valid. %s", path, key, ProjectPaths.RULE);
  }

  /** A package configuration with the keys that every compilation has. */
  private static SqrlConfig packageJson() {
    var config = SqrlConfig.createCurrentVersion();
    config.setProperty("script.main", "main.sqrl");
    config.setProperty("test-runner.snapshot-folder", "snapshots");
    config.setProperty("test-runner.test-folder", "tests");
    return config;
  }
}
