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
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class ServerConfigUtil {

  @SuppressWarnings("unchecked")
  @SneakyThrows
  public static ServerConfig mergeConfigs(
      ServerConfig serverConfig, Map<String, Object> configOverrides) {
    if (isEmpty(configOverrides)) {
      return serverConfig;
    }
    var config = ((ObjectNode) MAPPER.valueToTree(serverConfig)).deepCopy();
    JsonMergeUtils.merge(config, MAPPER.valueToTree(configOverrides));
    var json = MAPPER.treeToValue(config, Map.class);
    return new ServerConfig(new JsonObject(json));
  }

  /**
   * Maps a JSON object field to a configuration object using a constructor that accepts JsonObject.
   * If the field is missing or null, uses the provided default supplier.
   *
   * @param json the source JSON object
   * @param fieldName the field name to extract
   * @param ctor constructor function that takes JsonObject and returns the config object
   * @param defaultVal supplier for default value when field is missing or null
   * @param <T> the type of configuration object
   * @return the mapped configuration object
   */
  public static <T> T mapField(
      JsonObject json, String fieldName, Function<JsonObject, T> ctor, Supplier<T> defaultVal) {
    var fieldValue = json.getJsonObject(fieldName);
    if (fieldValue == null) {
      return defaultVal.get();
    }
    try {
      return ctor.apply(fieldValue);
    } catch (Exception e) {
      log.warn(
          "Failed to parse configuration field '{}', using default: {}", fieldName, e.getMessage());
      return defaultVal.get();
    }
  }

  /**
   * Maps a JSON object field to a configuration object using a constructor that accepts JsonObject.
   * If the field is missing or null, creates the object with an empty JsonObject.
   *
   * @param json the source JSON object
   * @param fieldName the field name to extract
   * @param ctor constructor function that takes JsonObject and returns the config object
   * @param <T> the type of configuration object
   * @return the mapped configuration object
   */
  public static <T> T mapFieldWithEmptyDefault(
      JsonObject json, String fieldName, Function<JsonObject, T> ctor) {
    return mapField(json, fieldName, ctor, () -> ctor.apply(new JsonObject()));
  }

  /**
   * Maps a JSON object field to a configuration object using a constructor that accepts JsonObject.
   * If the field is missing or null, returns null.
   *
   * @param json the source JSON object
   * @param fieldName the field name to extract
   * @param ctor constructor function that takes JsonObject and returns the config object
   * @param <T> the type of configuration object
   * @return the mapped configuration object or null if field is missing
   */
  public static <T> T mapFieldWithNullDefault(
      JsonObject json, String fieldName, Function<JsonObject, T> ctor) {
    return mapField(json, fieldName, ctor, () -> null);
  }
}
