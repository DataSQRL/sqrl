package com.datasqrl.util;

import com.datasqrl.serializer.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SqrlObjectMapper {
  public static final ObjectMapper INSTANCE = Deserializer.INSTANCE
      .getJsonMapper();
  public static final ObjectMapper YAML_INSTANCE = Deserializer.INSTANCE
      .getYamlMapper();
}
