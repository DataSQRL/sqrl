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
package com.datasqrl.serializer;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.function.Function;
import lombok.Getter;

@Getter
public class JacksonDeserializer<T> extends StdDeserializer<T> {

  private final String typeKey;
  private final Function<T, String> typeFunction;
  private final Class<T> superType;

  public JacksonDeserializer(Class<T> superType, String typeKey, Function<T, String> typeFunction) {
    super(superType);
    this.typeKey = typeKey;
    this.typeFunction = typeFunction;
    this.superType = superType;
  }

  @Override
  public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    final var mapper = (ObjectMapper) jsonParser.getCodec();
    final JsonNode node = mapper.readTree(jsonParser);

    Preconditions.checkArgument(
        node.get(typeKey) != null, "Object has no type [%s]: %s", typeKey, node);
    var type = node.get(typeKey).asText();

    ServiceLoader<T> serviceLoader = ServiceLoader.load(superType);
    for (T subType : serviceLoader) {
      if (typeFunction.apply(subType) != null && typeFunction.apply(subType).equals(type)) {
        return (T) mapper.treeToValue(node, subType.getClass());
      }
    }
    throw new IllegalArgumentException("Could not find class for type: " + type);
  }
}
