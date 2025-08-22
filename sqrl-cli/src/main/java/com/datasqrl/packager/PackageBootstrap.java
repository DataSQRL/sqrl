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

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.ScriptConfig;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class PackageBootstrap {
  ErrorCollector errors;

  public static Optional<List<Path>> findPackageFile(Path rootDir, List<Path> packageFiles) {
    if (!packageFiles.isEmpty()) {
      return Optional.of(packageFiles.stream().map(rootDir::resolve).toList());
    }

    var defaultPkg = rootDir.resolve(SqrlConstants.DEFAULT_PACKAGE);

    return Files.isRegularFile(defaultPkg) ? Optional.of(List.of(defaultPkg)) : Optional.empty();
  }

  /**
   * Initializes the {@link PackageJson} by merging all package.json configuration files, looking up
   * the default, and potentially adding the main script and graphql file as config options to it.
   *
   * <p>At the end of the bootstrap, the {@link PackageJson} is fully initialized and ready to be
   * used.
   *
   * @param rootDir
   * @param packageFiles
   * @param files
   * @param withRun
   * @return
   */
  @SneakyThrows
  public PackageJson bootstrap(
      Path rootDir, List<Path> packageFiles, Path[] files, boolean withRun) {
    ErrorCollector errors = this.errors.withLocation(ErrorPrefix.CONFIG).resolve("package");

    Optional<List<Path>> existingPackage = findPackageFile(rootDir, packageFiles);

    // Create package.json from project root if exists
    List<Path> configFiles = new ArrayList<>();
    existingPackage.ifPresent(configFiles::addAll);
    // Could not find any package json
    PackageJson packageJson =
        withRun
            ? ConfigLoaderUtils.loadUnresolvedRunConfig(errors, configFiles)
            : ConfigLoaderUtils.loadUnresolvedConfig(errors, configFiles);

    // Override main and graphql if they are specified as command line arguments
    Optional<Path> mainScript =
        (files.length > 0 && files[0].getFileName().toString().toLowerCase().endsWith(".sqrl"))
            ? Optional.of(files[0])
            : Optional.empty();
    Optional<Path> graphQLSchemaFile =
        (files.length > 1) ? Optional.of(files[1]) : Optional.empty();

    ScriptConfig scriptConfig = packageJson.getScriptConfig();
    boolean isMainScriptSet = scriptConfig.getMainScript().isPresent();
    boolean isGraphQLSet = scriptConfig.getGraphql().isPresent();

    // Set main script if not already set and if it's a regular file
    if (mainScript.isPresent() && Files.isRegularFile(relativize(rootDir, mainScript))) {
      scriptConfig.setMainScript(mainScript.get().toString());
    } else if (!isMainScriptSet && mainScript.isPresent()) {
      errors.fatal("Main script is not a regular file: %s", mainScript.get());
    } else if (!isMainScriptSet && files.length > 0) {
      errors.fatal("Main script is not a sqrl script: %s", files[0].getFileName().toString());
    } else if (!isMainScriptSet && mainScript.isEmpty()) {
      errors.fatal("No main sqrl script specified");
    }

    // Set GraphQL schema file if not already set and if it's a regular file
    if (graphQLSchemaFile.isPresent()
        && Files.isRegularFile(relativize(rootDir, graphQLSchemaFile))) {
      scriptConfig.setGraphql(graphQLSchemaFile.get().toString());
    } else if (!isGraphQLSet && graphQLSchemaFile.isPresent()) {
      errors.fatal("GraphQL schema file is not a regular file: %s", graphQLSchemaFile.get());
    }

    return packageJson;
  }

  private Path relativize(Path rootDir, Optional<Path> path) {
    return path.get().isAbsolute() ? path.get() : rootDir.resolve(path.get());
  }
}
