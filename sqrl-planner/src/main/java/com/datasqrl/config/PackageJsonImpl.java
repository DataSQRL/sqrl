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

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;

public class PackageJsonImpl implements PackageJson {

  public static final String ENGINES_PROPERTY = "engines";
  public static final String ENABLED_ENGINES_KEY = "enabled-engines";
  public static final String DISCOVERY_KEY = "discovery";
  public static final String SCRIPT_KEY = "script";
  public static final String COMPILER_KEY = "compiler";
  public static final String CONNECTORS_KEY = "connectors";
  public static final String CONFIG_KEY = "config";
  public static final String TEST_RUNNER_KEY = "test-runner";

  @Getter private final SqrlConfig sqrlConfig;
  private final Set<String> userEngineConfigurations;

  public PackageJsonImpl() {
    this(SqrlConfig.createCurrentVersion());
  }

  public PackageJsonImpl(SqrlConfig sqrlConfig) {
    this(sqrlConfig, Set.of());
  }

  public PackageJsonImpl(SqrlConfig sqrlConfig, Set<String> userEngineConfigurations) {
    this.sqrlConfig = sqrlConfig;
    this.userEngineConfigurations = Set.copyOf(userEngineConfigurations);
  }

  @Override
  public List<String> getEnabledEngines() {
    return sqrlConfig.asList(ENABLED_ENGINES_KEY, String.class).get();
  }

  @Override
  public void setEnabledEngines(List<String> enabledEngines) {
    sqrlConfig.setProperty(ENABLED_ENGINES_KEY, enabledEngines);
  }

  @Override
  public void removeDisabledEngineConfigurations(ErrorCollector errors) {
    var enabledEngines = new HashSet<>(getEnabledEngines());
    var engineConfigurations = sqrlConfig.getSubConfig(ENGINES_PROPERTY);
    var engineNamesToRemove = new ArrayList<String>();

    for (String engineName : engineConfigurations.getKeys()) {
      if (!enabledEngines.contains(engineName)) {
        engineNamesToRemove.add(engineName);
      }
    }

    engineNamesToRemove.forEach(engineConfigurations::removeProperty);

    var userEngineConfigurationsToRemove =
        engineNamesToRemove.stream().filter(userEngineConfigurations::contains).toList();
    if (!userEngineConfigurationsToRemove.isEmpty()) {
      errors.warn(
          "Removed configurations for engines not listed in 'enabled-engines': %s.",
          userEngineConfigurationsToRemove);
    }

    if (!engineConfigurations.getKeys().iterator().hasNext()) {
      sqrlConfig.removeProperty(ENGINES_PROPERTY);
    }
  }

  @Override
  public EnginesConfigImpl getEngines() {
    return new EnginesConfigImpl(sqrlConfig.getSubConfig(ENGINES_PROPERTY));
  }

  @Override
  public ConnectorsConfig getConnectors() {
    return new ConnectorsConfigImpl(sqrlConfig.getSubConfig(CONNECTORS_KEY));
  }

  @Override
  public DiscoveryConfigImpl getDiscovery() {
    return new DiscoveryConfigImpl(sqrlConfig.getSubConfig(DISCOVERY_KEY));
  }

  @Override
  public void toFile(Path path, boolean pretty) {
    sqrlConfig.toFile(path, pretty);
  }

  @Override
  public ScriptConfig getScriptConfig() {
    return new ScriptConfigImpl(sqrlConfig.getSubConfig(SCRIPT_KEY));
  }

  @Override
  public CompilerConfigImpl getCompilerConfig() {
    return new CompilerConfigImpl(sqrlConfig.getSubConfig(COMPILER_KEY));
  }

  @Override
  public int getVersion() {
    return sqrlConfig.getVersion();
  }

  @Override
  public TestRunnerConfiguration getTestConfig() {
    return new TestRunnerConfigImpl(sqrlConfig.getSubConfig(TEST_RUNNER_KEY));
  }

  @Override
  public String toString() {
    return "PackageJsonImpl{" + sqrlConfig + '}';
  }
}
