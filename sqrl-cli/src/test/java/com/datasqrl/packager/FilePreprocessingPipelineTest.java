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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
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
            Set.of(),
            new SourceFiles());

    pipeline.run(projectDir, ErrorCollector.root());

    assertThat(buildDir.resolve("api/schema.graphqls")).hasContent("type Query { ping: String }");
    assertThat(buildDir.resolve("api/operations.graphql")).hasContent("query Ping { ping }");
    assertThat(buildDir.resolve("api/apps/SalesChart.html")).hasContent("<html>chart</html>");
    assertThat(buildDir.resolve("api/apps/Orders.HTML")).hasContent("<html>orders</html>");
    assertThat(buildDir.resolve("api/apps/README.md")).doesNotExist();
  }

  private static void write(Path file, String content) throws IOException {
    Files.createDirectories(file.getParent());
    Files.writeString(file, content);
  }
}
