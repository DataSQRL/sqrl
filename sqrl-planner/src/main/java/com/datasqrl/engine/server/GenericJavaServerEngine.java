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
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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

  private String serverConfig() throws IOException, StreamReadException, DatabindException {
    ServerConfig serverConfig;
    try (var input = getClass().getResourceAsStream("/templates/server-config.json")) {
      Map<String, Object> json = objectMapper.readValue(input, Map.class);
      serverConfig = new ServerConfig(new JsonObject(json));
    }

    var configOverrides = objectMapper.valueToTree(engineConfig.getConfig());
    var mergedConfig = deepMerge(objectMapper.valueToTree(serverConfig), configOverrides);

    return objectMapper.copy().writerWithDefaultPrettyPrinter().writeValueAsString(mergedConfig);
  }

  public static JsonNode deepMerge(JsonNode mainNode, JsonNode updateNode) {
    if (mainNode instanceof ObjectNode && updateNode instanceof ObjectNode) {
      ObjectNode merged = ((ObjectNode) mainNode).deepCopy();
      updateNode
          .fields()
          .forEachRemaining(
              entry -> {
                JsonNode existingValue = merged.get(entry.getKey());
                if (existingValue != null
                    && existingValue.isObject()
                    && entry.getValue().isObject()) {
                  merged.set(entry.getKey(), deepMerge(existingValue, entry.getValue()));
                } else {
                  merged.set(entry.getKey(), entry.getValue());
                }
              });
      return merged;
    }
    return updateNode;
  }
}
