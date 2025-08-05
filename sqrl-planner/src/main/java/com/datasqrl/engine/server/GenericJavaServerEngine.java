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
package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineFeature.NO_CAPABILITIES;
import static com.datasqrl.graphql.SqrlObjectMapper.MAPPER;

import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.relational.DuckDBEngineFactory;
import com.datasqrl.engine.database.relational.SnowflakeEngineFactory;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.config.ServerConfigUtil;
import io.vertx.core.json.JsonObject;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** A generic java server engine. */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  private final EngineConfig engineConfig;
  private final List<String> enabledEngines;

  public GenericJavaServerEngine(String engineName, PackageJson packageJson) {
    super(engineName, EngineType.SERVER, NO_CAPABILITIES);
    engineConfig = packageJson.getEngines().getEngineConfigOrEmpty(engineName);
    enabledEngines = packageJson.getEnabledEngines();
  }

  @Override
  @SneakyThrows
  public EnginePhysicalPlan plan(com.datasqrl.planner.dag.plan.ServerStagePlan serverPlan) {
    serverPlan.getFunctions().stream()
        .filter(fct -> fct.getExecutableQuery() == null)
        .forEach(
            fct -> {
              throw new IllegalStateException("Function has not been planned: " + fct);
            });

    return new ServerPhysicalPlan(
        serverPlan.getFunctions(),
        serverPlan.getMutations(),
        List.of(new DeploymentArtifact("-config.json", serverConfig())));
  }

  @SneakyThrows
  private String serverConfig() {
    var mergedConfig = ServerConfigUtil.mergeConfigs(readDefaultConfig(), engineConfig.getConfig());
    return MAPPER.copy().writer(new PrettyPrinter()).writeValueAsString(mergedConfig);
  }

  @SneakyThrows
  ServerConfig readDefaultConfig() {
    ServerConfig serverConfig;
    try (var input = getClass().getResourceAsStream("/templates/server-config.json")) {
      var json = MAPPER.readValue(input, JsonObject.class);
      disableConfigIfNeeded(json, DuckDBEngineFactory.ENGINE_NAME, "duckDbConfig");
      disableConfigIfNeeded(json, SnowflakeEngineFactory.ENGINE_NAME, "snowflakeConfig");

      serverConfig = new ServerConfig(json);
    }
    return serverConfig;
  }

  private void disableConfigIfNeeded(JsonObject json, String engineName, String configName) {
    if (!enabledEngines.contains(engineName)) {
      json.putNull(configName);
    }
  }
}
