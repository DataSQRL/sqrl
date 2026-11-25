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
package com.datasqrl;

import static com.datasqrl.SnapshotTestSupport.getResourcesDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.config.ProjectPaths;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Compiles a project in a workspace next to a sibling project, with paths in its package file that
 * stay inside the workspace or leave it. The compiler reads anything in the workspace, and fails on
 * a path that leads outside without reading the file.
 */
class ProjectPathContainmentTest {

  private static final Path USECASE_DIR = getResourcesDirectory("usecases");
  private static final String OUTSIDE_CONTENT = "first line of a file outside the workspace";

  @RegisterExtension final CliCompileTestExtension cli = new CliCompileTestExtension();

  @TempDir Path tempDir;

  private Path workspaceDir;
  private Path projectDir;
  private Path outsideDir;

  @BeforeEach
  void setUp() throws IOException {
    // The test extension uses the parent of the project as the workspace
    workspaceDir = tempDir.resolve("workspace");
    projectDir = workspaceDir.resolve("project");
    FileUtils.copyDirectory(USECASE_DIR.resolve("mcp-apps").toFile(), projectDir.toFile());
    var sharedDir = workspaceDir.resolve("shared");
    FileUtils.copyFile(
        projectDir.resolve("operations.graphql").toFile(),
        sharedDir.resolve("ops.graphql").toFile());
    FileUtils.copyDirectory(
        projectDir.resolve("apps").toFile(), sharedDir.resolve("apps").toFile());

    outsideDir = Files.createDirectories(tempDir.resolve("outside"));
    Files.writeString(
        outsideDir.resolve("ops.graphql"),
        "# " + OUTSIDE_CONTENT + "\nquery Outside { Sales(limit: 1) { region } }\n");
    Files.writeString(outsideDir.resolve("notes.txt"), OUTSIDE_CONTENT + "\n");
    Files.createDirectories(outsideDir.resolve("apps"));
    Files.writeString(
        outsideDir.resolve("apps/Outside.html"), "<html>" + OUTSIDE_CONTENT + "</html>\n");
  }

  @ParameterizedTest(name = "{0} = {1}")
  @CsvSource({
    "script.main, ../../outside/notes.txt",
    "script.main, {outside}/notes.txt",
    "script.api.v1.operations, ../../outside/ops.graphql",
    "script.api.v1.operations, {outside}/ops.graphql",
    "script.api.v1.operations, {workspace}/shared/ops.graphql",
    "script.api.v1.mcp-apps, ../../outside/apps",
    "script.api.v1.openapi, ../../outside/notes.txt",
    "test-runner.test-folder, ../../outside",
    "test-runner.snapshot-folder, ../../outside",
    "script.include.ext.package, ../../outside/package.json"
  })
  void givenAbsolutePathOrPathOutsideWorkspace_whenCompile_thenFailsNamingKeyAndValue(
      String key, String path) throws IOException {
    var value =
        path.replace("{outside}", outsideDir.toAbsolutePath().toString())
            .replace("{workspace}", workspaceDir.toAbsolutePath().toString());
    setPath(key, value);

    var hook = cli.execute(projectDir, "compile", "package.json");

    assertThat(hook.isFailed()).as(hook.getMessages()).isTrue();
    assertThat(hook.getMessages())
        .contains("Value [" + value + "] for key [" + key + "] is not valid. " + ProjectPaths.RULE)
        .doesNotContain(OUTSIDE_CONTENT);
    assertThat(cli.getPlanDir().resolve("vertx.json")).doesNotExist();
  }

  @Test
  void givenMcpAppsOutsideWorkspaceWithoutMcp_whenCompile_thenFailsNamingKeyAndValue()
      throws IOException {
    var packageFile = projectDir.resolve("package.json");
    var json = (ObjectNode) SqrlObjectMapper.INSTANCE.readTree(packageFile.toFile());
    json.putObject("compiler").putObject("api").putArray("protocols").add("GRAPHQL").add("REST");
    SqrlObjectMapper.INSTANCE.writeValue(packageFile.toFile(), json);
    setPath("script.api.v1.mcp-apps", "../../outside/apps");

    var hook = cli.execute(projectDir, "compile", "package.json");

    assertThat(hook.isFailed()).as(hook.getMessages()).isTrue();
    assertThat(hook.getMessages())
        .contains(
            "Value [../../outside/apps] for key [script.api.v1.mcp-apps] is not valid. "
                + ProjectPaths.RULE)
        .doesNotContain(OUTSIDE_CONTENT);
  }

  @Test
  void givenPathsToSiblingInWorkspace_whenCompile_thenReadsThem() throws IOException {
    Files.delete(projectDir.resolve("operations.graphql"));
    FileUtils.deleteDirectory(projectDir.resolve("apps").toFile());
    setPath("script.main", "apps/../mcp-apps.sqrl");
    setPath("script.api.v1.operations", "../shared/ops.graphql");
    setPath("script.api.v1.mcp-apps", "../shared/apps");

    var hook = cli.execute(projectDir, "compile", "package.json");

    assertThat(hook.isSuccess()).as(hook.getMessages()).isTrue();
    assertThat(cli.getPlanDir().resolve("vertx.json")).content().contains("GetSales");
    assertThat(cli.getPlanDir().resolve("vertx-v1-mcp-app-SalesChart.html")).exists();
  }

  @Test
  void givenOperationsLinkedToSiblingInWorkspace_whenCompile_thenFollowsLink() throws IOException {
    Files.delete(projectDir.resolve("operations.graphql"));
    Files.createSymbolicLink(
        projectDir.resolve("operations.graphql"), Path.of("../shared/ops.graphql"));

    var hook = cli.execute(projectDir, "compile", "package.json");

    assertThat(hook.isSuccess()).as(hook.getMessages()).isTrue();
    assertThat(hook.getMessages()).doesNotContain("Skipped the symbolic link");
    assertThat(cli.getPlanDir().resolve("vertx.json")).content().contains("GetSales");
  }

  @Test
  void givenOperationsLinkedToOutsideWorkspace_whenCompile_thenFailsNamingKey() throws IOException {
    Files.delete(projectDir.resolve("operations.graphql"));
    Files.createSymbolicLink(
        projectDir.resolve("operations.graphql"), outsideDir.resolve("ops.graphql"));

    var hook = cli.execute(projectDir, "compile", "package.json");

    assertThat(hook.isFailed()).as(hook.getMessages()).isTrue();
    assertThat(hook.getMessages())
        .contains(
            "Value [operations.graphql] for key [script.api.v1.operations] is not valid. "
                + ProjectPaths.RULE)
        .doesNotContain(OUTSIDE_CONTENT);
  }

  @Test
  void givenUnconfiguredFileLinkedToOutsideWorkspace_whenCompile_thenSkipsLink()
      throws IOException {
    Files.createSymbolicLink(
        projectDir.resolve("extra.graphql"), outsideDir.resolve("ops.graphql"));

    var hook = cli.execute(projectDir, "compile", "package.json");

    assertThat(hook.isSuccess()).as(hook.getMessages()).isTrue();
    assertThat(hook.getMessages())
        .contains(
            "Skipped the symbolic link 'extra.graphql', because it leads outside the workspace")
        .doesNotContain(OUTSIDE_CONTENT);
    assertThat(cli.getBuildDir().resolve("extra.graphql")).doesNotExist();
    assertThat(cli.getPlanDir().resolve("vertx.json")).content().doesNotContain(OUTSIDE_CONTENT);
  }

  @Test
  void givenStandaloneIncludeWithMainOutsideItsRoot_whenCompile_thenFailsNamingInclude()
      throws IOException {
    var includingDir = workspaceDir.resolve("kafka-include-subscribe-compile");
    var includeDir = workspaceDir.resolve("kafka-include-shared");
    FileUtils.copyDirectory(
        USECASE_DIR.resolve("kafka-include-subscribe-compile").toFile(), includingDir.toFile());
    FileUtils.copyDirectory(
        USECASE_DIR.resolve("kafka-include-shared").toFile(), includeDir.toFile());
    var includePackage = includeDir.resolve("package.json");
    var json = (ObjectNode) SqrlObjectMapper.INSTANCE.readTree(includePackage.toFile());
    ((ObjectNode) json.get("script")).put("main", "../../outside/notes.txt");
    SqrlObjectMapper.INSTANCE.writeValue(includePackage.toFile(), json);

    var hook = cli.execute(includingDir, "compile", "package-standalone.json");

    assertThat(hook.isFailed()).as(hook.getMessages()).isTrue();
    assertThat(hook.getMessages())
        .contains(
            "Value [../../outside/notes.txt] for key [script.main] in the package file"
                + " '../kafka-include-shared/package.json' of include 'shared' is not valid. The"
                + " main script of a standalone include must be inside the included project.")
        .doesNotContain(OUTSIDE_CONTENT);
  }

  /** Sets a key of the project's package file, as a list for operations. */
  private void setPath(String key, String value) throws IOException {
    var packageFile = projectDir.resolve("package.json");
    var json = (ObjectNode) SqrlObjectMapper.INSTANCE.readTree(packageFile.toFile());
    var segments = List.of(key.split("\\."));
    var parent = json;
    for (var segment : segments.subList(0, segments.size() - 1)) {
      parent = parent.has(segment) ? (ObjectNode) parent.get(segment) : parent.putObject(segment);
    }
    var field = segments.get(segments.size() - 1);
    if (field.equals("operations")) {
      parent.putArray(field).add(value);
    } else {
      parent.put(field, value);
    }
    SqrlObjectMapper.INSTANCE.writeValue(packageFile.toFile(), json);
  }
}
