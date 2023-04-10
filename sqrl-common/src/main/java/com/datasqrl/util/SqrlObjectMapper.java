package com.datasqrl.util;

import com.datasqrl.util.serializer.Deserializer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class SqrlObjectMapper {
  public static ObjectMapper INSTANCE = new Deserializer()
      .getJsonMapper();
}
