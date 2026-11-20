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
package com.datasqrl.packager;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorMessage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FilePreprocessingPipelineTest {

  @TempDir Path tempDir;

  @Test
  void givenProjectFiles_whenRun_thenCopiesApiFilesAndMcpAppsToBuildAtTheirPaths()
      throws IOException {
    var projectDir = tempDir.resolve("project");
    var buildDir = projectDir.resolve("build");
    write(projectDir.resolve("api/schema.graphqls"), "type Query { ping: String }");
    write(projectDir.resolve("api/operations.graphql"), "query Ping { ping }");
    write(projectDir.resolve("api/apps/SalesChart.html"), "<html>chart</html>");
    write(projectDir.resolve("api/apps/Orders.HTML"), "<html>orders</html>");
    write(projectDir.resolve("api/apps/README.md"), "# Apps");
    var pipeline =
        new FilePreprocessingPipeline(
            new WorkspacePaths(tempDir, buildDir, buildDir.resolve("deploy"), projectDir),
            buildDir,
            Set.of());

    pipeline.run(projectDir, ErrorCollector.root());

    assertThat(buildDir.resolve("api/schema.graphqls")).hasContent("type Query { ping: String }");
    assertThat(buildDir.resolve("api/operations.graphql")).hasContent("query Ping { ping }");
    assertThat(buildDir.resolve("api/apps/SalesChart.html")).hasContent("<html>chart</html>");
    assertThat(buildDir.resolve("api/apps/Orders.HTML")).hasContent("<html>orders</html>");
    assertThat(buildDir.resolve("api/apps/README.md")).doesNotExist();
  }

  @Test
  void givenLinksThatLeaveWorkspace_whenRun_thenSkipsThemAndWarns() throws IOException {
    var workspaceDir = tempDir.resolve("workspace");
    var projectDir = workspaceDir.resolve("project");
    var buildDir = projectDir.resolve("build");
    var outsideDir = tempDir.resolve("outside");
    write(projectDir.resolve("ops.graphql"), "query Ping { ping }");
    write(outsideDir.resolve("secret.graphql"), "query Secret { secret }");
    write(outsideDir.resolve("shared/ops.graphql"), "query Shared { shared }");
    Files.createDirectories(projectDir.resolve("api"));
    Files.createSymbolicLink(
        projectDir.resolve("api/secret.graphql"), outsideDir.resolve("secret.graphql"));
    Files.createSymbolicLink(projectDir.resolve("shared"), outsideDir.resolve("shared"));
    var errors = ErrorCollector.root();

    newPipeline(workspaceDir, projectDir).run(projectDir, errors);

    assertThat(buildDir.resolve("ops.graphql")).hasContent("query Ping { ping }");
    assertThat(buildDir.resolve("api/secret.graphql")).doesNotExist();
    assertThat(buildDir.resolve("shared")).doesNotExist();
    assertThat(warnings(errors))
        .containsExactlyInAnyOrder(
            "Skipped the symbolic link 'api/secret.graphql', because it leads outside the"
                + " workspace",
            "Skipped the symbolic link 'shared', because it leads outside the workspace");
  }

  @Test
  void givenLinksInsideWorkspace_whenRun_thenFollowsThem() throws IOException {
    var workspaceDir = tempDir.resolve("workspace");
    var projectDir = workspaceDir.resolve("project");
    var buildDir = projectDir.resolve("build");
    write(projectDir.resolve("api/schema.graphqls"), "type Query { ping: String }");
    write(workspaceDir.resolve("shared/ops.graphql"), "query Shared { shared }");
    Files.createSymbolicLink(
        projectDir.resolve("schema.graphqls"), projectDir.resolve("api/schema.graphqls"));
    Files.createSymbolicLink(projectDir.resolve("api-v1"), projectDir.resolve("api"));
    // A link to a sibling folder in the workspace, by a relative target
    Files.createSymbolicLink(projectDir.resolve("shared"), Path.of("../shared"));
    var errors = ErrorCollector.root();

    newPipeline(workspaceDir, projectDir).run(projectDir, errors);

    assertThat(buildDir.resolve("schema.graphqls")).hasContent("type Query { ping: String }");
    assertThat(buildDir.resolve("api-v1/schema.graphqls"))
        .hasContent("type Query { ping: String }");
    assertThat(buildDir.resolve("shared/ops.graphql")).hasContent("query Shared { shared }");
    assertThat(warnings(errors)).isEmpty();
  }

  @Test
  void givenIncludeWithLinkThatLeavesWorkspace_whenRunInclude_thenSkipsItAndWarns()
      throws IOException {
    var workspaceDir = tempDir.resolve("workspace");
    var projectDir = workspaceDir.resolve("project");
    var buildDir = projectDir.resolve("build");
    var includeDir = workspaceDir.resolve("shared-lib");
    write(projectDir.resolve("api/schema.graphqls"), "type Query { ping: String }");
    write(includeDir.resolve("lib.graphql"), "query Lib { lib }");
    write(tempDir.resolve("outside/secret.graphql"), "query Secret { secret }");
    Files.createSymbolicLink(
        includeDir.resolve("project.graphqls"), projectDir.resolve("api/schema.graphqls"));
    Files.createSymbolicLink(
        includeDir.resolve("secret.graphql"), tempDir.resolve("outside/secret.graphql"));
    var errors = ErrorCollector.root();

    newPipeline(workspaceDir, projectDir).runInclude(includeDir, "lib", errors);

    assertThat(buildDir.resolve("lib/lib.graphql")).hasContent("query Lib { lib }");
    assertThat(buildDir.resolve("lib/project.graphqls")).hasContent("type Query { ping: String }");
    assertThat(buildDir.resolve("lib/secret.graphql")).doesNotExist();
    assertThat(warnings(errors))
        .containsExactly(
            "Skipped the symbolic link 'secret.graphql' of the included project 'lib', because it"
                + " leads outside the workspace");
  }

  private FilePreprocessingPipeline newPipeline(Path workspaceDir, Path projectDir) {
    var buildDir = projectDir.resolve("build");
    return new FilePreprocessingPipeline(
        new WorkspacePaths(workspaceDir, buildDir, buildDir.resolve("deploy"), projectDir),
        buildDir,
        Set.of());
  }

  private static List<String> warnings(ErrorCollector errors) {
    return StreamSupport.stream(errors.spliterator(), false)
        .filter(ErrorMessage::isWarning)
        .map(ErrorMessage::getMessage)
        .toList();
  }

  private static void write(Path file, String content) throws IOException {
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }
}
