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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import picocli.CommandLine.Option;

public abstract class BaseOsProcessManagerCmd extends BaseCmd {

  @Option(
      names = {"-r", "--project-root"},
      description =
          "Project root folder. Must be a relative path. If omitted, it is inferred from the package path(s), falling back to \"/workspace\".")
  protected Optional<Path> projectRoot = Optional.empty();

  @Option(
      names = {"-b", "--build"},
      description =
          "Build folder for any output generated during build. Must be a relative path within the project root."
              + " Resolved relative to the specified or inferred project root. Default: \"<project-root>/build\".\n")
  protected Optional<Path> buildFolder = Optional.empty();

  @Option(
      names = {"-t", "--target"},
      description =
          "Target folder for deployment artifacts and plans. Must be a relative path within the project root."
              + " Resolved relative to the specified or inferred project root. Default: \"<project-root>/build/deploy\".\n")
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
    if (projectRoot.isEmpty()) {
      return cli.workspaceDir;
    }

    return getFullProjectRoot(projectRoot.get());
  }

  protected Path getBuildDir() {
    return buildFolder
        .map(path -> resolveGivenCliDir(path, "Build"))
        .orElseGet(() -> getProjectRoot().resolve(SqrlConstants.BUILD_DIR_NAME));
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

  /**
   * Resolves the project root against the CLI workspace directory.
   *
   * @param projectRoot relative project root path
   * @return project root resolved from the workspace directory
   * @throws IllegalArgumentException if the path is absolute or does not resolve to an existing
   *     directory
   */
  protected Path getFullProjectRoot(Path projectRoot) {
    if (projectRoot.isAbsolute()) {
      throw new IllegalArgumentException(
          "Project root must be a relative path, got: " + projectRoot);
    }

    var fullProjectRoot = cli.workspaceDir.resolve(projectRoot);
    if (!Files.isDirectory(fullProjectRoot)) {
      throw new IllegalArgumentException(
          "Project root does not exist or not a directory: " + fullProjectRoot);
    }

    return fullProjectRoot;
  }

  private Path resolveGivenCliDir(Path dir, String dirType) {
    if (!cli.internalTestExec && dir.isAbsolute()) {
      throw new IllegalArgumentException(dirType + " folder must be a relative path, got: " + dir);
    }

    var root = getProjectRoot();
    var directory = root.resolve(dir);
    if (!cli.internalTestExec && !directory.normalize().startsWith(root.normalize())) {
      throw new IllegalArgumentException(
          dirType
              + " folder '"
              + dir
              + "' resolves to '"
              + directory.normalize()
              + "', which is outside project root '"
              + root.normalize()
              + "'");
    }

    return directory;
  }
}
