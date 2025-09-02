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

import static com.datasqrl.graphql.SqrlObjectMapper.MAPPER;
import static org.apache.commons.lang3.ObjectUtils.isEmpty;

import com.datasqrl.util.JsonMergeUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class ServerConfigUtil {

  /**
   * Merges server configuration with override values using Jackson.
   *
   * @param serverConfig base server configuration
   * @param configOverrides map of configuration overrides
   * @return merged and validated server configuration
   */
  @SneakyThrows
  public static ServerConfig mergeConfigs(
      ServerConfig serverConfig, Map<String, Object> configOverrides) {
    if (isEmpty(configOverrides)) {
      return serverConfig;
    }
    var config = ((ObjectNode) MAPPER.valueToTree(serverConfig)).deepCopy();
    JsonMergeUtils.merge(config, MAPPER.valueToTree(configOverrides));
    return MAPPER.treeToValue(config, ServerConfig.class).validated();
  }

  /**
   * Creates a ServerConfig from a configuration map using Jackson deserialization.
   *
   * @param configMap map containing configuration values
   * @return deserialized and validated ServerConfig instance
   */
  @SneakyThrows
  public static ServerConfig fromConfigMap(Map<String, Object> configMap) {
    return MAPPER.convertValue(configMap, ServerConfig.class).validated();
  }
}
