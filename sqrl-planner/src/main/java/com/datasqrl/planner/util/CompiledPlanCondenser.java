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
package com.datasqrl.planner.util;

import static com.datasqrl.util.ConfigLoaderUtils.MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Condenses a Flink compiled plan JSON to include only the below essential information:
 *
 * <ul>
 *   <li>Flink version
 *   <li>Node id, type, description
 *   <li>Edge source, target
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class CompiledPlanCondenser {

  public static String condense(String compiledPlanJson) {
    try {
      var root = MAPPER.readTree(compiledPlanJson);
      var condensed = MAPPER.createObjectNode();

      if (root.has("flinkVersion")) {
        condensed.put("flinkVersion", root.get("flinkVersion").asText());
      }

      condenseNodes(root).ifPresent(condensedNodes -> condensed.set("nodes", condensedNodes));
      condenseEdges(root).ifPresent(condensedEdges -> condensed.set("edges", condensedEdges));

      return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(condensed);

    } catch (Exception e) {
      log.debug("Failed to condense compiled plan", e);
      // If parsing fails, return empty JSON
      return "{}";
    }
  }

  private static Optional<ArrayNode> condenseNodes(JsonNode root) {
    if (!root.has("nodes")) {
      return Optional.empty();
    }

    var condensedNodes = MAPPER.createArrayNode();

    for (var node : root.get("nodes")) {
      var condensedNode = MAPPER.createObjectNode();

      if (node.has("id")) {
        condensedNode.put("id", node.get("id").asInt());
      }

      if (node.has("type")) {
        condensedNode.put("type", node.get("type").asText());
      }

      if (node.has("description")) {
        condensedNode.put("description", node.get("description").asText());
      }

      condensedNodes.add(condensedNode);
    }

    return Optional.of(condensedNodes);
  }

  private static Optional<ArrayNode> condenseEdges(JsonNode root) {
    if (!root.has("edges")) {
      return Optional.empty();
    }

    var condensedEdges = MAPPER.createArrayNode();

    for (var edge : root.get("edges")) {
      var condensedEdge = MAPPER.createObjectNode();

      if (edge.has("source")) {
        condensedEdge.put("source", edge.get("source").asInt());
      }

      if (edge.has("target")) {
        condensedEdge.put("target", edge.get("target").asInt());
      }

      condensedEdges.add(condensedEdge);
    }

    return Optional.of(condensedEdges);
  }
}
