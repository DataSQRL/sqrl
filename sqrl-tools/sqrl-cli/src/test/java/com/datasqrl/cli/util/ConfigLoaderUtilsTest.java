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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderUtilsTest {

  @TempDir Path tempDir;

  @Test
  void loadPackageJson_shouldLoadSuccessfully_whenDeployDirIsValid() {
    // Arrange
    Path deployDir = tempDir.resolve("deploy");
    Path parentDir = tempDir;
    Path packageJsonPath = parentDir.resolve("package.json");
    deployDir.toFile().mkdirs();

    // Create dummy package.json file
    try {
      Files.writeString(packageJsonPath, "{ \"name\": \"test\" }");
    } catch (IOException e) {
      fail("Failed to create package.json");
    }

    // Spy on SqrlConfig to verify delegation
    var spySqrlConfig = mockStatic(SqrlConfig.class);
    PackageJson expectedPackageJson = mock(PackageJson.class);

    spySqrlConfig
        .when(
            () ->
                SqrlConfig.fromFilesPackageJson(
                    any(ErrorCollector.class), eq(List.of(packageJsonPath))))
        .thenReturn(expectedPackageJson);

    // Act
    PackageJson result = ConfigLoaderUtils.loadPackageJson(deployDir);

    // Assert
    assertThat(result).isSameAs(expectedPackageJson);

    spySqrlConfig.close();
  }

  @Test
  void loadPackageJson_shouldThrow_whenDeployDirDoesNotExist() {
    Path invalidDir = tempDir.resolve("nonexistent");

    assertThatThrownBy(() -> ConfigLoaderUtils.loadPackageJson(invalidDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("deploy dir does not exist");
  }

  @Test
  void loadFlinkConfig_shouldLoadSuccessfully_whenConfigExists() throws IOException {
    // Arrange
    Path planDir = tempDir.resolve("plan");
    planDir.toFile().mkdirs();

    Path flinkConfig = planDir.resolve("flink-config.yaml");
    String yamlContent = "jobmanager.rpc.address: localhost";

    Files.writeString(flinkConfig, yamlContent);

    // Act
    Configuration conf = ConfigLoaderUtils.loadFlinkConfig(planDir);

    // Assert
    assertThat(conf).isNotNull();
    assertThat(conf.getString("jobmanager.rpc.address", null)).isEqualTo("localhost");
  }

  @Test
  void loadFlinkConfig_shouldThrow_whenPlanDirDoesNotExist() {
    Path invalidDir = tempDir.resolve("doesNotExist");

    assertThatThrownBy(() -> ConfigLoaderUtils.loadFlinkConfig(invalidDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("plan dir does not exist");
  }

  @Test
  void loadFlinkConfig_shouldThrow_whenFlinkConfigYamlMissing() {
    Path planDir = tempDir.resolve("planWithoutYaml");
    planDir.toFile().mkdirs();

    assertThatThrownBy(() -> ConfigLoaderUtils.loadFlinkConfig(planDir))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'flink-config.yaml' does not found");
  }
}
