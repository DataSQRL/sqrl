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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ResourceUtils;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@CommandLine.Command(name = "init", description = "Initializes an empty SQRL project")
public class InitCmd extends BaseCmd {

  private static final String INIT_PROJECT_DIR = "templates/init-project";
  private static final String PROJECT_NAME_PLACEHOLDER = "__projectname__";

  private final MustacheFactory mustacheFactory = new DefaultMustacheFactory();

  @Parameters(index = "0", description = "Project type. Valid values: ${COMPLETION-CANDIDATES}")
  ProjectType projectType = ProjectType.STREAM;

  @Parameters(
      index = "1",
      description =
          "Project name. The SQRL script and package config files will be named like this")
  String projectName;

  @Option(
      names = {"--batch"},
      description = "Use BATCH mode in Flink")
  boolean batch = false;

  @Override
  protected void runInternal(ErrorCollector errors) {
    try {
      initProject(() -> cli.rootDir);
    } catch (Exception e) {
      throw errors.exception("Project initialization failed: %s", e);
    }
  }

  @SneakyThrows
  void initProject(Supplier<Path> targetRoot) {
    var resources = ResourceUtils.listResourceFiles(INIT_PROJECT_DIR);

    var variables = loadVariables(projectType);
    variables.put(
        "exec-mode",
        batch ? RuntimeExecutionMode.BATCH.name() : RuntimeExecutionMode.STREAMING.name());

    for (var resource : resources) {
      try (var is = ResourceUtils.getResourceAsStream(resource)) {
        var targetPath = getTargetPath(targetRoot.get(), resource);
        var fileName = targetPath.getFileName().toString();

        Files.createDirectories(targetPath.getParent());

        if (fileName.endsWith(".mustache")) {
          writeResolvedContent(is, targetPath, variables);

        } else {
          Files.copy(is, targetPath);
        }
      }
    }
  }

  @SneakyThrows
  private void writeResolvedContent(
      InputStream is, Path targetPath, Map<String, Object> variables) {

    var finalName = FilenameUtils.removeExtension(targetPath.getFileName().toString());
    var finalTargetPath = targetPath.getParent().resolve(finalName);

    try (var reader = new InputStreamReader(is, UTF_8);
        var writer = Files.newBufferedWriter(finalTargetPath, UTF_8)) {

      var mustache = mustacheFactory.compile(reader, finalName);
      mustache.execute(writer, variables).flush();
    }
  }

  private static Map<String, Object> loadVariables(ProjectType projectType) {
    var props = ResourceUtils.loadProperties("init-project.properties");
    var prefix = projectType.name().toLowerCase() + '.';

    return props.entrySet().stream()
        .filter(e -> e.getKey().toString().startsWith(prefix))
        .collect(
            Collectors.toMap(
                e -> e.getKey().toString().substring(prefix.length()),
                e -> e.getValue().toString()));
  }

  private Path getTargetPath(Path targetRoot, String resourcePath) {
    var subProjectPath =
        resourcePath
            .replace(INIT_PROJECT_DIR, "")
            .replace(PROJECT_NAME_PLACEHOLDER, projectName)
            .replaceFirst("^/", ""); // Strip leading slash

    return targetRoot.resolve(subProjectPath);
  }

  public enum ProjectType {
    STREAM,
    DATASET,
    API
  }
}
