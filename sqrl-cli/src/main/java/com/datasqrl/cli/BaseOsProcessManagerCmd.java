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
import com.datasqrl.env.GlobalEnvironmentStore;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.OsProcessManager;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

public abstract class BaseOsProcessManagerCmd extends BaseCmd {

  @Mixin protected ProjectRootOption projectRootOption = new ProjectRootOption();

  @Option(
      names = {"-b", "--build"},
      description =
          "Subfolder of \"<project-root>/build\" for build output."
              + " The value must be a relative path that remains within that build folder."
              + " For example, \"--build development\" writes to \"<project-root>/build/development\"."
              + " If omitted, output is written directly to \"<project-root>/build\"."
              + " This setting determines the default target folder when \"--target\" is omitted.\n")
  protected Optional<Path> buildFolder = Optional.empty();

  @Option(
      names = {"-t", "--target"},
      description =
          "Folder for deployment artifacts and plans. The value must be a relative path that remains within \"<project-root>\"."
              + " Unlike \"--build\", an explicit value is resolved directly from the project root:"
              + " \"--target artifacts\" writes to \"<project-root>/artifacts\", independent of \"--build\"."
              + " If omitted, the target is \"<build-folder>/deploy\"."
              + " For example, \"--build development\" defaults the target to \"<project-root>/build/development/deploy\".\n")
  protected Optional<Path> targetFolder = Optional.empty();

  @Override
  protected void setupEnvVars() {
    var basePath = getTargetDir();
    var dataPath = basePath.resolve("flink/data").toAbsolutePath().toString();
    var libPath = basePath.resolve("flink/lib").toAbsolutePath().toString();

    GlobalEnvironmentStore.put("DATA_PATH", dataPath);
    GlobalEnvironmentStore.put("UDF_PATH", libPath);
  }

  @Override
  protected void teardown() {
    if (!cli.internalTestExec) {
      getOsProcessManager().teardown(getBuildDir());
    }
  }

  protected Path getProjectRoot() {
    return projectRootOption.resolve(cli.workspaceDir);
  }

  protected Path getBuildDir() {
    return buildFolder
        .map(path -> resolveGivenCliDir(path, "Build", getBuildBaseDir(), "build folder"))
        .orElseGet(this::getBuildBaseDir);
  }

  protected Path getBuildBaseDir() {
    return getProjectRoot().resolve(SqrlConstants.BUILD_DIR_NAME);
  }

  protected Path getTargetDir() {
    return targetFolder
        .map(path -> resolveGivenCliDir(path, "Target"))
        .orElseGet(() -> getBuildDir().resolve(SqrlConstants.DEPLOY_DIR_NAME));
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

  private Path resolveGivenCliDir(Path dir, String dirType) {
    return resolveGivenCliDir(dir, dirType, getProjectRoot(), "project root");
  }

  private Path resolveGivenCliDir(Path dir, String dirType, Path root, String rootDescription) {
    if (!cli.internalTestExec && dir.isAbsolute()) {
      throw new IllegalArgumentException(dirType + " folder must be a relative path, got: " + dir);
    }

    var directory = root.resolve(dir);
    if (!cli.internalTestExec && !directory.normalize().startsWith(root.normalize())) {
      throw new IllegalArgumentException(
          dirType
              + " folder '"
              + dir
              + "' resolves to '"
              + directory.normalize()
              + "', which is outside "
              + rootDescription
              + " '"
              + root.normalize()
              + "'");
    }

    return directory;
  }
}
