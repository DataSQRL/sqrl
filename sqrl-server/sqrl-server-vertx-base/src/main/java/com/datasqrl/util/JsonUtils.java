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
package com.datasqrl.util;

import com.datasqrl.env.GlobalEnvironmentStore;
import com.datasqrl.flinkrunner.utils.EnvVarResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.json.jackson.VertxModule;
import jakarta.annotation.Nullable;
import java.util.Map;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.registerModule(new VertxModule());
  }

  public static ObjectMapper getMapperWithEnvVarResolver() {
    return getMapperWithEnvVarResolver(System.getenv());
  }

  public static ObjectMapper getMapperWithEnvVarResolver(@Nullable Map<String, String> env) {
    var finalEnv = env != null ? env : GlobalEnvironmentStore.getAll();
    var resolver = EnvVarResolver.builder().envVars(finalEnv).strict(false).build();

    return resolver.initObjectMapper(MAPPER.copy());
  }

  public static void merge(ObjectNode target, ObjectNode update) {
    update
        .properties()
        .forEach(
            entry -> {
              var existing = target.get(entry.getKey());
              if (existing instanceof ObjectNode existingObject && entry.getValue().isObject()) {
                merge(existingObject, (ObjectNode) entry.getValue());
              } else {
                target.set(entry.getKey(), entry.getValue());
              }
            });
  }
}
