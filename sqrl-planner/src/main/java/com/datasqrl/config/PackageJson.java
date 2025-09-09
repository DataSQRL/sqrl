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
import com.datasqrl.graphql.server.operation.ApiProtocol;
import com.datasqrl.planner.PredicatePushdownRules;
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

  void setEnabledEngines(List<String> enabledEngines);

  EnginesConfig getEngines();

  ConnectorsConfig getConnectors();

  DiscoveryConfig getDiscovery();

  void toFile(Path path, boolean pretty);

  ScriptConfig getScriptConfig();

  CompilerConfig getCompilerConfig();

  int getVersion();

  boolean hasScriptKey();

  TestRunnerConfiguration getTestConfig();

  interface CompilerConfig {

    boolean compileFlinkPlan();

    PredicatePushdownRules predicatePushdownRules();

    CostModel getCostModel();

    ExplainConfig getExplain();

    CompilerApiConfig getApiConfig();

    String getLogger();

    boolean isExtendedScalarTypes();
  }

  interface CompilerApiConfig {

    EnumSet<ApiProtocol> getProtocols();

    boolean isGraphQLProtocolOnly();

    boolean generateOperations();

    boolean isAddOperationsPrefix();

    int getMaxResultDepth();

    int getDefaultLimit();
  }

  interface ExplainConfig {

    boolean isSql();

    boolean isLogical();

    boolean isPhysical();

    boolean isSorted();
  }

  interface ScriptConfig {

    Optional<String> getMainScript();

    List<ScriptApiConfig> getScriptApiConfigs();

    Optional<String> getGraphql();

    List<String> getOperations();

    Map<String, Object> getConfig();

    void setMainScript(String script);

    void setGraphql(String graphql);
  }

  interface ScriptApiConfig {

    String getVersion();

    String getSchema();

    List<String> getOperations();
  }

  interface EnginesConfig {

    Optional<EngineConfig> getEngineConfig(String engineId);

    EngineConfig getEngineConfigOrErr(String engineId);

    EngineConfig getEngineConfigOrEmpty(String engineId);
  }

  interface EngineConfig {

    String getEngineName();

    default String getSetting(String key) {
      return getSetting(key, Optional.empty());
    }

    String getSetting(String key, Optional<String> defaultValue);

    Optional<String> getSettingOptional(String key);

    Map<String, Object> getConfig();

    boolean isEmpty();
  }

  @AllArgsConstructor
  @Getter
  class EmptyEngineConfig implements EngineConfig {
    String engineName;

    @Override
    public String getSetting(String key, Optional<String> defaultValue) {
      return defaultValue.orElseThrow(
          () ->
              new IllegalArgumentException(
                  "Engine " + engineName + " does not have configuration options"));
    }

    @Override
    public Optional<String> getSettingOptional(String key) {
      return Optional.empty();
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

  interface DiscoveryConfig {

    DataDiscoveryConfig getDataDiscoveryConfig();
  }

  interface DataDiscoveryConfig {

    TablePattern getTablePattern(String defaultTablePattern);

    ErrorCollector getErrors();
  }
}
