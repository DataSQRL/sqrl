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
import com.datasqrl.graphql.server.operation.ApiProtocols;
import com.datasqrl.planner.analyzer.cost.CostModel;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

public interface PackageJson {

  List<String> getEnabledEngines();

  void setPipeline(List<String> pipeline);

  EnginesConfig getEngines();

  DiscoveryConfig getDiscovery();

  DependenciesConfig getDependencies();

  void toFile(Path path, boolean pretty);

  ScriptConfig getScriptConfig();

  CompilerConfig getCompilerConfig();

  int getVersion();

  boolean hasScriptKey();

  TestRunnerConfiguration getTestConfig();

  interface CompilerConfig {

    boolean compileFlinkPlan();

    CostModel getCostModel();

    ExplainConfig getExplain();

    CompilerApiConfig getApiConfig();

    String getLogger();

    boolean isExtendedScalarTypes();
  }

  interface CompilerApiConfig {

    EnumSet<ApiProtocols> getProtocols();

    boolean isGraphQLProtocolOnly();

    boolean generateOperations();

    boolean isAddOperationsPrefix();

    int getMaxResultDepth();
  }

  interface ExplainConfig {

    boolean isText();

    boolean isSql();

    boolean isLogical();

    boolean isPhysical();

    boolean isSorted();

    boolean isVisual();
  }

  interface ScriptConfig {

    Optional<String> getMainScript();

    Optional<String> getGraphql();

    List<String> getOperations();

    void setMainScript(String script);

    void setGraphql(String graphql);
  }

  interface EnginesConfig {

    Optional<EngineConfig> getEngineConfig(String engineId);

    EngineConfig getEngineConfigOrErr(String engineId);

    EngineConfig getEngineConfigOrEmpty(String engineId);
  }

  interface EngineConfig {

    String getEngineName();

    String getSetting(String key, Optional<String> defaultValue);

    ConnectorsConfig getConnectors();

    Map<String, Object> getConfig();

    boolean isEmpty();
  }

  @AllArgsConstructor
  @Getter
  static class EmptyEngineConfig implements EngineConfig {
    String engineName;

    @Override
    public String getSetting(String key, Optional<String> defaultValue) {
      return defaultValue.orElseThrow(
          () ->
              new IllegalArgumentException(
                  "Engine " + engineName + " does not have configuration options"));
    }

    @Override
    public ConnectorsConfig getConnectors() {
      return null;
    }

    @Override
    public Map<String, Object> getConfig() {
      return Map.of();
    }

    @Override
    public boolean isEmpty() {
      return true;
    }
  }

  interface DependenciesConfig {

    void addDependency(String key, Dependency dep);

    Optional<Dependency> getDependency(String profile);

    Map<String, ? extends Dependency> getDependencies();
  }

  interface DiscoveryConfig {

    DataDiscoveryConfig getDataDiscoveryConfig();
  }

  interface DataDiscoveryConfig {

    TablePattern getTablePattern(String defaultTablePattern);

    ErrorCollector getErrors();
  }
}
