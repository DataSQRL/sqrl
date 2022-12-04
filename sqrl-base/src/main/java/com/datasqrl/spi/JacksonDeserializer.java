/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.spi;

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
    final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
    final JsonNode node = mapper.readTree(jsonParser);

    Preconditions.checkArgument(node.get(typeKey) != null, "Object has no type [%s]: %s", typeKey,
        node);
    String type = node.get(typeKey).asText();

    ServiceLoader<T> serviceLoader = ServiceLoader.load(superType);
    for (T subType : serviceLoader) {
      if (typeFunction.apply(subType).equals(type)) {
        return (T) mapper.treeToValue(node, subType.getClass());
      }
    }
    throw new IllegalArgumentException("Could not find class for type: " + type);
  }
}
