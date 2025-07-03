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
package com.datasqrl.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

/** Utility class to load different kind of configurations during CLI process execution. */
public final class ConfigLoaderUtils {

  /**
   * Loads SQRL {@code package.json} from an already compiled project.
   *
   * @param deployDir deployment directory of a compiled SQRL project
   * @return the loaded {@link PackageJson}
   */
  public static PackageJson loadPackageJson(Path deployDir, ExecutionGoal goal) {
    checkArgument(
        Files.isDirectory(deployDir),
        "Failed to load " + SqrlConstants.PACKAGE_JSON + ", deploy dir does not exist.");

    if (goal == ExecutionGoal.RUN) {
      return SqrlConfig.fromFilesPackageJsonWithRun(
          ErrorCollector.root(),
          List.of(deployDir.getParent().resolve(SqrlConstants.PACKAGE_JSON)));
    }

    return SqrlConfig.fromFilesPackageJson(
        ErrorCollector.root(), List.of(deployDir.getParent().resolve(SqrlConstants.PACKAGE_JSON)));
  }

  /**
   * Loads Flink configuration from a {@code flink-config.yaml} file that is assumed to exist inside
   * the given plan directory.
   *
   * @param planDir plan directory that contains tha YAML
   * @return Flink {@link Configuration} with loaded config
   * @throws IOException if any internal file operation fails
   */
  public static Configuration loadFlinkConfig(final Path planDir) throws IOException {
    checkArgument(
        Files.isDirectory(planDir), "Failed to load Flink config, plan dir does not exist.");

    var confFile = planDir.resolve("flink-config.yaml");
    checkArgument(
        confFile.toFile().exists() && confFile.toFile().isFile(),
        "Failed to load Flink config, 'flink-config.yaml' does not found.");

    var tempDir = Files.createTempDirectory("flink-conf");
    try {
      var targetFile = tempDir.resolve(GlobalConfiguration.FLINK_CONF_FILENAME);
      Files.copy(confFile, targetFile);

      return GlobalConfiguration.loadConfiguration(tempDir.toString());
    } finally {
      FileUtils.forceDelete(tempDir.toFile());
    }
  }
}
