package com.datasqrl.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;

public class SerializerUtil {

  private static final ObjectMapper jsonMapper = new ObjectMapper()
      .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @SneakyThrows
  public static <O> String toJson(O obj) {
    return jsonMapper.writeValueAsString(obj);
  }


}
