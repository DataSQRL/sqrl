package com.datasqrl.graphql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Configures a Jackson ObjectMapper for JSON serialization/deserialization.
 */
public class SqrlObjectMapper {

  public static final ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    mapper.registerModule(new JavaTimeModule());
  }
}
