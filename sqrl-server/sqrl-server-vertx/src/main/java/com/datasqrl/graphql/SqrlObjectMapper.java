package com.datasqrl.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class SqrlObjectMapper {

  public static ObjectMapper mapper = createObjectMapper();
  public static ObjectMapper createObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    registerModules(objectMapper);
    return objectMapper;
  }

  private static void registerModules(ObjectMapper mapper) {
    SimpleModule module = new SimpleModule();

    mapper.registerModule(new JavaTimeModule())
        .registerModule((new Jdk8Module()).configureAbsentsAsNulls(true))
        .registerModule(module)
        .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

}
