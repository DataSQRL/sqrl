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
package com.datasqrl.ai.tool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import lombok.NonNull;

public class FunctionUtil {

  public static String toJsonString(List<APIFunction> tools) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.valueToTree(tools));
  }

  /**
   * Adds/overwrites the context fields on the message with the provided context.
   *
   * @param arguments
   * @param contextKeys
   * @param context
   * @param mapper
   * @return
   */
  public static JsonNode addOrOverrideContext(
      JsonNode arguments,
      @NonNull Set<String> contextKeys,
      @NonNull Context context,
      @NonNull ObjectMapper mapper) {
    // Create a copy of the original JsonNode to add context
    ObjectNode copyJsonNode;
    if (arguments == null || arguments.isEmpty()) {
      copyJsonNode = mapper.createObjectNode();
    } else {
      copyJsonNode = arguments.deepCopy();
    }
    // Add context
    for (String contextKey : contextKeys) {
      Object value = context.get(contextKey);
      if (value == null) {
        throw new IllegalArgumentException("Missing context field: " + contextKey);
      }
      copyJsonNode.putPOJO(contextKey, value);
    }
    return copyJsonNode;
  }
}
