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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import picocli.CommandLine.Option;

/** The {@code -r, --project-root} option, mixed into the commands that work on a project. */
public class ProjectRootOption {

  @Option(
      names = {"-r", "--project-root"},
      description =
          "Project root folder. Must be a relative path. If omitted, it is inferred from the"
              + " package path(s), or is \"/workspace\" for a command that takes none.")
  Optional<Path> projectRoot = Optional.empty();

  boolean isSet() {
    return projectRoot.isPresent();
  }

  /**
   * Resolves the given project root, or returns the workspace directory if none was given.
   *
   * @param workspaceDir CLI workspace directory
   * @return project root resolved from the workspace directory
   */
  Path resolve(Path workspaceDir) {
    return projectRoot.map(root -> resolve(workspaceDir, root)).orElse(workspaceDir);
  }

  /**
   * Resolves a project root against the CLI workspace directory.
   *
   * @param workspaceDir CLI workspace directory
   * @param projectRoot relative project root path
   * @return project root resolved from the workspace directory
   * @throws IllegalArgumentException if the path is absolute or does not resolve to an existing
   *     directory
   */
  static Path resolve(Path workspaceDir, Path projectRoot) {
    if (projectRoot.isAbsolute()) {
      throw new IllegalArgumentException(
          "Project root must be a relative path, got: " + projectRoot);
    }

    var fullProjectRoot = workspaceDir.resolve(projectRoot);
    if (!Files.isDirectory(fullProjectRoot)) {
      throw new IllegalArgumentException(
          "Project root does not exist or not a directory: " + fullProjectRoot);
    }

    return fullProjectRoot;
  }
}
