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
import static com.datasqrl.graphql.config.ServerConfigUtil.mergeConfigs;

import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.QueryEngineConfigConverter;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.EnginePhysicalPlan.ArtifactType;
import com.datasqrl.engine.EnginePhysicalPlan.DeploymentArtifact;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.planner.dag.plan.ServerStagePlan;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** A generic java server engine. */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  private final EngineConfig engineConfig;
  private final QueryEngineConfigConverter configConverter;

  GenericJavaServerEngine(
      String engineName, EngineConfig engineConfig, QueryEngineConfigConverter configConverter) {
    super(engineName, EngineType.SERVER, NO_CAPABILITIES);
    this.engineConfig = engineConfig;
    this.configConverter = configConverter;
  }

  @Override
  @SneakyThrows
  public EnginePhysicalPlan plan(ServerStagePlan serverPlan) {
    validateFunctions(serverPlan.functions());

    var execFnPlan = serverPlan.execFnFactory().getPlan();
    var artifacts = new ArrayList<DeploymentArtifact>();
    artifacts.add(new DeploymentArtifact("-config.json", serverConfig()));

    if (!execFnPlan.functions().isEmpty()) {
      artifacts.add(new DeploymentArtifact("-exec-functions.ser", execFnPlan));
      artifacts.add(new DeploymentArtifact("-exec-functions.json", execFnPlan, ArtifactType.JSON));
    }

    return new ServerPhysicalPlan(serverPlan.functions(), serverPlan.mutations(), artifacts);
  }

  private void validateFunctions(List<SqrlTableFunction> functions) {
    var invalidFunctions =
        functions.stream().filter(fn -> fn.getExecutableQuery() == null).toList();

    if (!invalidFunctions.isEmpty()) {
      throw new IllegalStateException(
          "Found function(s) that has not been planned: " + invalidFunctions);
    }
  }

  @SneakyThrows
  private String serverConfig() {
    var mergedConfig = mergeConfigs(readDefaultConfig(), engineConfig.getConfig());
    return MAPPER.copy().writer(new PrettyPrinter()).writeValueAsString(mergedConfig);
  }

  @SneakyThrows
  ServerConfig readDefaultConfig() {
    try (var input = getClass().getResourceAsStream("/templates/server-config.json")) {
      var serverConfNode = (ObjectNode) MAPPER.readTree(input);
      configConverter.convertConfigsToJson().forEach(serverConfNode::setAll);

      return MAPPER.treeToValue(serverConfNode, ServerConfig.class).validated();
    }
  }
}
