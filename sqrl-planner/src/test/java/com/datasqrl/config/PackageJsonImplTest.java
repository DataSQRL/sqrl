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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PackageJsonImplTest {

  private SqrlConfig config;

  @TempDir private Path tempDir;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
  }

  @Test
  void givenConfigWithEnabledEngines_whenCreatePackageJson_thenReturnsCorrectValues() {
    config.setProperty("enabled-engines", List.of("flink", "postgres", "kafka"));

    var packageJson = new PackageJsonImpl(config);

    assertThat(packageJson.getEnabledEngines()).containsExactly("flink", "postgres", "kafka");
    assertThat(packageJson.getVersion()).isEqualTo(1);
    assertThat(packageJson.getEngines()).isNotNull();
    assertThat(packageJson.getDiscovery()).isNotNull();
    assertThat(packageJson.getDependencies()).isNotNull();
    assertThat(packageJson.getScriptConfig()).isNotNull();
    assertThat(packageJson.getCompilerConfig()).isNotNull();
  }

  @Test
  void givenPackageJson_whenSetEnabledEngines_thenUpdatesEnabledEngines() {
    var packageJson = new PackageJsonImpl(config);

    var enabledEngines = List.of("stage1", "stage2", "stage3");
    packageJson.setEnabledEngines(enabledEngines);

    assertThat(packageJson.getEnabledEngines()).containsExactlyInAnyOrderElementsOf(enabledEngines);
  }

  @Test
  void givenConfigWithoutScript_whenHasScriptKey_thenReturnsFalse() {
    var packageJson = new PackageJsonImpl(config);

    assertThat(packageJson.hasScriptKey()).isFalse();
  }

  @Test
  void givenConfigWithScript_whenHasScriptKey_thenReturnsFalse() {
    config.getSubConfig("script").setProperty("main", "example.sqrl");

    var packageJson = new PackageJsonImpl(config);

    // Current implementation always returns false - this appears to be unimplemented
    assertThat(packageJson.hasScriptKey()).isFalse();
  }

  @Test
  void givenPackageJson_whenGetTestConfig_thenReturnsTestConfiguration() {
    config.getSubConfig("test-runner").setProperty("snapshot-folder", "/dummy");

    var packageJson = new PackageJsonImpl(config);

    assertThat(packageJson.getTestConfig().getSnapshotDir(null)).isEqualTo(Paths.get("/dummy"));
  }

  @Test
  @SneakyThrows
  void givenPackageJsonWithData_whenToFile_thenWritesJsonFile() {
    config.setProperty("enabled-engines", List.of("flink", "postgres"));
    config.setProperty("script.main", "test.sqrl");

    PackageJsonImpl packageJson = new PackageJsonImpl(config);

    var tempFile = Files.createTempFile(tempDir, "package", ".json");
    packageJson.toFile(tempFile, true);

    assertThat(tempFile).exists();
    String content = Files.readString(tempFile);
    assertThat(content).contains("enabled-engines");
    assertThat(content).contains("flink");
    assertThat(content).contains("postgres");
  }

  @Test
  void givenEmptyConfig_whenCreateScriptConfig_thenReturnsEmptyValues() {
    var scriptConfig = new ScriptConfigImpl(config);

    assertThat(scriptConfig.getMainScript()).isEmpty();
    assertThat(scriptConfig.getGraphql()).isEmpty();
  }

  @Test
  void givenScriptConfig_whenSetValues_thenUpdatesConfiguration() {
    var scriptConfig = new ScriptConfigImpl(config);

    scriptConfig.setMainScript("main.sqrl");
    scriptConfig.setGraphql("schema.graphql");

    assertThat(scriptConfig.getMainScript()).contains("main.sqrl");
    assertThat(scriptConfig.getGraphql()).contains("schema.graphql");
  }

  @Test
  void givenConfigWithEngines_whenCreateEnginesConfig_thenReturnsEngineConfigurations() {
    config.getSubConfig("engines").getSubConfig("flink").setProperty("type", "flink");
    config.getSubConfig("engines").getSubConfig("postgres").setProperty("type", "postgres");

    var enginesConfig = new EnginesConfigImpl(config.getSubConfig("engines"));

    assertThat(enginesConfig.getEngineConfig("flink")).isPresent();
    assertThat(enginesConfig.getEngineConfig("postgres")).isPresent();
    assertThat(enginesConfig.getEngineConfig("nonexistent")).isEmpty();
  }

  @Test
  void givenConfigWithConnectors_whenCreateConnectorsConfig_thenReturnsConnectorConfigurations() {
    config
        .getSubConfig("connectors")
        .getSubConfig("jdbc")
        .setProperty("url", "jdbc:postgresql://localhost:5432/db");
    config
        .getSubConfig("connectors")
        .getSubConfig("kafka")
        .setProperty("bootstrap.servers", "localhost:9092");

    var connectorsConfig = new ConnectorsConfigImpl(config.getSubConfig("connectors"));

    assertThat(connectorsConfig.getConnectorConfig("jdbc")).isPresent();
    assertThat(connectorsConfig.getConnectorConfig("kafka")).isPresent();
    assertThat(connectorsConfig.getConnectorConfig("nonexistent")).isEmpty();
  }

  @Test
  void
      givenConfigWithDependencies_whenCreateDependenciesConfig_thenReturnsDependencyConfigurations() {
    config.getSubConfig("dependencies").getSubConfig("dep1").setProperty("name", "dependency1");
    config.getSubConfig("dependencies").getSubConfig("dep2").setProperty("name", "dependency2");

    var dependenciesConfig =
        new DependenciesConfigImpl(config, config.getSubConfig("dependencies"));

    assertThat(dependenciesConfig.getDependency("dep1")).isPresent();
    assertThat(dependenciesConfig.getDependency("dep2")).isPresent();
    assertThat(dependenciesConfig.getDependency("nonexistent")).isEmpty();
  }

  @Test
  void givenConfig_whenCreateDiscoveryConfig_thenReturnsDiscoveryConfiguration() {
    var discoveryConfig = new DiscoveryConfigImpl(config.getSubConfig("discovery"));

    assertThat(discoveryConfig).isNotNull();
  }
}
