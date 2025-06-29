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
package com.datasqrl.graphql.config;

import static com.datasqrl.graphql.SqrlObjectMapper.mapper;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

import com.datasqrl.util.JsonMergeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ServerConfigUtil {

  @SuppressWarnings("unchecked")
  @SneakyThrows
  public static ServerConfig mergeConfigs(
      ServerConfig serverConfig, Map<String, Object> configOverrides) {
    if (isEmpty(configOverrides)) {
      return serverConfig;
    }
    var config = ((ObjectNode) mapper.valueToTree(serverConfig)).deepCopy();
    JsonMergeUtils.merge(config, mapper.valueToTree(configOverrides));
    var json = mapper.treeToValue(config, Map.class);
    return new ServerConfig(new JsonObject(json));
  }
}
