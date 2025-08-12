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

import java.nio.file.Path;
import java.util.List;
import lombok.Getter;

public class PackageJsonImpl implements PackageJson {

  public static final String ENGINES_PROPERTY = "engines";
  public static final String ENABLED_ENGINES_KEY = "enabled-engines";
  public static final String DISCOVERY_KEY = "discovery";
  public static final String DEPENDENCIES_KEY = "dependencies";
  public static final String SCRIPT_KEY = "script";
  public static final String COMPILER_KEY = "compiler";
  public static final String CONNECTORS_KEY = "connectors";
  public static final String CONFIG_KEY = "config";
  public static final String TEST_RUNNER_KEY = "test-runner";

  @Getter private final SqrlConfig sqrlConfig;

  public PackageJsonImpl() {
    this(SqrlConfig.createCurrentVersion());
  }

  public PackageJsonImpl(SqrlConfig sqrlConfig) {
    this.sqrlConfig = sqrlConfig;
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
  public DependenciesConfigImpl getDependencies() {
    return new DependenciesConfigImpl(sqrlConfig, sqrlConfig.getSubConfig(DEPENDENCIES_KEY));
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
  public boolean hasScriptKey() {
    // ScriptConfiguration.SCRIPT_KEY
    return false;
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
