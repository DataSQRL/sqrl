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

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.NameUtil;
import java.nio.file.Path;

public record WorkspacePaths(Path workspaceDir, Path buildDir, Path targetDir, Path projectRoot) {

  /** Returns the rule for the paths that the project configures. */
  public ProjectPaths projectPaths() {
    return new ProjectPaths(workspaceDir, projectRoot);
  }

  /**
   * Resolves a path that the package file configures, relative to the project root, see {@link
   * ProjectPaths}. A path inside the project resolves to its copy in the build directory, which the
   * packager preprocessed, and a path elsewhere in the workspace to the file itself.
   *
   * @param key the configuration key, for the error message
   * @param path the path as configured
   * @param errors the collector to fail with if the path leads outside the workspace
   * @return the path to read
   */
  public Path resolveConfiguredPath(String key, String path, ErrorCollector errors) {
    var resolved = projectPaths().resolve(key, path, errors).toAbsolutePath();
    var root = projectRoot.toAbsolutePath().normalize();

    return resolved.startsWith(root) ? buildDir.resolve(root.relativize(resolved)) : resolved;
  }

  public Path getIncludePath(String namespace) {
    return NameUtil.namepath2Path(buildDir, NamePath.of(namespace));
  }

  public Path getUdfPath() {
    return targetDir.resolve(SqrlConstants.FLINK_ASSETS_DIR).resolve(SqrlConstants.LIB_DIR);
  }

  public Path getDataPath() {
    return targetDir.resolve(SqrlConstants.FLINK_ASSETS_DIR).resolve(SqrlConstants.DATA_DIR);
  }
}
