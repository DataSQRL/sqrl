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

import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.config.ServerConfigUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.VertxModule;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** A generic java server engine. */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  private final EngineConfig engineConfig;
  private final ObjectMapper objectMapper;

  public GenericJavaServerEngine(
      String engineName, EngineConfig engineConfig, ObjectMapper objectMapper) {
    super(engineName, EngineType.SERVER, NO_CAPABILITIES);
    this.engineConfig = engineConfig;
    this.objectMapper = objectMapper;
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
    var mergedConfig =
        ServerConfigUtil.mergeConfigs(objectMapper, readDefaultConfig(), engineConfig.getConfig());
    return objectMapper.writer(new PrettyPrinter()).writeValueAsString(mergedConfig);
  }

  @SneakyThrows
  ServerConfig readDefaultConfig() {
    ServerConfig serverConfig;
    try (var input = getClass().getResourceAsStream("/templates/server-config.json")) {
      var json =
          objectMapper.copy().registerModule(new VertxModule()).readValue(input, JsonObject.class);
      serverConfig = new ServerConfig(json);
    }
    return serverConfig;
  }
}
