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

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.OsProcessManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import picocli.CommandLine.Option;

public abstract class BaseOsProcessManagerCmd extends BaseCmd {

  @Option(
      names = {"-p", "--project-base"},
      description = "Base folder of the project. Must be a relative path. Default: \"./\".")
  protected Optional<Path> projectRoot = Optional.empty();

  @Option(
      names = {"-t", "--target"},
      description =
          "Target folder for deployment artifacts and plans. Must be a relative path. Default: \"./build/deploy\".")
  protected Optional<Path> targetDir = Optional.empty();

  protected Path getProjectRoot() {
    if (projectRoot.isEmpty()) {
      return cli.workspaceDir;
    }

    var projRoot = projectRoot.get();
    if (projRoot.isAbsolute()) {
      throw new IllegalArgumentException("Project root must be a relative path");
    }

    projRoot = cli.workspaceDir.resolve(projRoot);
    if (!Files.isDirectory(projRoot)) {
      throw new IllegalArgumentException(
          "Project root does not exist or not a directory: " + projRoot);
    }

    return projRoot;
  }

  protected Path getBuildDir() {
    return getProjectRoot().resolve(SqrlConstants.BUILD_DIR_NAME);
  }

  protected Path getTargetDir() {
    if (targetDir.isPresent()) {
      var target = targetDir.get();
      if (!cli.internalTestExec && target.isAbsolute()) {
        throw new IllegalArgumentException("Target directory must be a relative path");
      }
      return cli.workspaceDir.resolve(target);
    }

    return getBuildDir().resolve(SqrlConstants.DEPLOY_DIR_NAME);
  }

  @Override
  protected void teardown() {
    if (!cli.internalTestExec) {
      getOsProcessManager().teardown(getBuildDir());
    }
  }

  protected OsProcessManager getOsProcessManager() {
    return new OsProcessManager(System.getenv());
  }

  /**
   * Loads the Flink configuration generated for the plan directory.
   *
   * @param planDir directory containing the compiled plan artifacts
   * @return loaded Flink configuration
   */
  @SneakyThrows
  protected Configuration getFlinkConfig(Path planDir) {
    return ConfigLoaderUtils.loadFlinkConfig(planDir);
  }
}
