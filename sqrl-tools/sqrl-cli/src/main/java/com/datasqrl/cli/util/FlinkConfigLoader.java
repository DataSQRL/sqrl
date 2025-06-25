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
package com.datasqrl.cli.util;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

public final class FlinkConfigLoader {

  public static Configuration fromYamlFile(final Path planDir) throws IOException {
    checkArgument(
        planDir.toFile().exists() == planDir.toFile().isDirectory(),
        "Failed to load Flink config, plan dir does not exist.");

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
