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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.VertxModule;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
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
    this.objectMapper = objectMapper.copy().registerModule(new VertxModule());
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
    var mergedConfig = mergeConfigs(readDefaultConfig(), patchJwtConfig(engineConfig.getConfig()));
    return objectMapper.writer(new PrettyPrinter()).writeValueAsString(mergedConfig);
  }

  private Map<String, Object> patchJwtConfig(Map<String, Object> source) {
    // apache configuration2 stores things in memory in a flatten format and it doesn't hold a copy
    // of the original configuration/format

    // FIXME this if a temporary fix, and must be reevaluated
    Object jwtAuthObj = source.get("jwtAuth");
    if (!(jwtAuthObj instanceof Map)) {
      return source; // nothing to patch
    }
    Map<String, Object> jwtAuth = (Map<String, Object>) jwtAuthObj;

    Object pubSecObj = jwtAuth.get("pubSecKeys");
    if (pubSecObj instanceof Map) {
      Map<String, Object> pubSecKeysMap = (Map<String, Object>) pubSecObj;

      /* ----- extract the two parallel lists ----- */
      List<?> algos = toList(pubSecKeysMap.get("algorithm"));
      List<?> buffers = toList(pubSecKeysMap.get("buffer"));

      /* ----- rebuild as List<Map<String,String>> ----- */
      List<Map<String, Object>> pubSecKeys = new ArrayList<>();
      int n = Math.max(algos.size(), buffers.size());
      for (int i = 0; i < n; i++) {
        Map<String, Object> entry = new LinkedHashMap<>();
        if (i < algos.size()) entry.put("algorithm", algos.get(i));
        if (i < buffers.size()) entry.put("buffer", buffers.get(i));
        pubSecKeys.add(entry);
      }

      jwtAuth.put("pubSecKeys", pubSecKeys); // **replace** old structure
    }

    /* ----------  ensure audience is a List  ---------- */
    Object jwtOptionsObj = jwtAuth.get("jwtOptions");
    if (jwtOptionsObj instanceof Map) {
      Map<String, Object> jwtOptions = (Map<String, Object>) jwtOptionsObj;
      Object audience = jwtOptions.get("audience");
      if (!(audience instanceof List)) {
        jwtOptions.put(
            "audience",
            audience == null ? Collections.emptyList() : Collections.singletonList(audience));
      }
    }

    return source; // patched in place
  }

  /* ----------  helper: coerce any value to List<?>  ---------- */
  private static List<?> toList(Object o) {
    if (o == null) return Collections.emptyList();
    if (o instanceof List) return (List<?>) o;
    if (o.getClass().isArray()) return Arrays.asList((Object[]) o);
    return Collections.singletonList(o);
  }

  @SneakyThrows
  ServerConfig readDefaultConfig() {
    ServerConfig serverConfig;
    try (var input = getClass().getResourceAsStream("/templates/server-config.json")) {
      var json = objectMapper.readValue(input, JsonObject.class);
      serverConfig = new ServerConfig(json);
    }
    return serverConfig;
  }

  @SneakyThrows
  ServerConfig mergeConfigs(ServerConfig serverConfig, Map<String, Object> configOverrides) {
    var mergedConfig =
        deepMerge(
            objectMapper.valueToTree(serverConfig), objectMapper.valueToTree(configOverrides));
    var json = objectMapper.treeToValue(mergedConfig, Map.class);
    return new ServerConfig(new JsonObject(json));
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
