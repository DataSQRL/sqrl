package com.datasqrl.util;

import com.datasqrl.serializer.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SqrlObjectMapper {
  public static ObjectMapper INSTANCE = new Deserializer()
      .getJsonMapper();
  public static ObjectMapper YAML_INSTANCE = new Deserializer()
      .getYamlMapper();
}
