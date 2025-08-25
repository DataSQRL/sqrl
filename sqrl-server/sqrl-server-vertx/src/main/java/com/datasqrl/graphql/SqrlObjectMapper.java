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
package com.datasqrl.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.json.jackson.VertxModule;
import jakarta.annotation.Nullable;
import java.util.Map;

/** Configures a Jackson ObjectMapper for JSON serialization/deserialization. */
public class SqrlObjectMapper {

  public static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    MAPPER.registerModule(new JavaTimeModule());
    MAPPER.registerModule(new VertxModule());
  }

  public static ObjectMapper getMapperWithEnvVarResolver(@Nullable Map<String, String> env) {
    var mapper = SqrlObjectMapper.MAPPER.copy();
    var module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer(env));
    mapper.registerModule(module);

    return mapper;
  }
}
