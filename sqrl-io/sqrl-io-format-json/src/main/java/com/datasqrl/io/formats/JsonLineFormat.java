/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.io.impl.InputPreview;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;

import com.google.auto.service.AutoService;
import lombok.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@AutoService(FormatFactory.class)
public class JsonLineFormat implements TextLineFormat {

  public static final String NAME = "json";
  public static final List<String> EXTENSIONS = List.of(NAME);

  @Override
  public List<String> getExtensions() {
    return EXTENSIONS;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Parser getParser(@NonNull SqrlConfig config) {
    return new JsonLineParser();
  }

  @Override
  public String getTableApiSerializerName() {
    return "flexible-json";
  }

  @Override
  public Writer getWriter(@NonNull SqrlConfig config) {
    final ObjectMapper mapper = SqrlObjectMapper.INSTANCE;
    return new Writer() {
      @Override
      public String write(Map<String, Object> record) throws Exception {
        return mapper.writeValueAsString(record);
      }
    };
  }

  @NoArgsConstructor
  public static class JsonLineParser implements TextLineFormat.Parser {

    private transient ObjectMapper mapper;

    @Override
    public Result parse(@NonNull String line) {
      if (mapper == null) {
        mapper = SqrlObjectMapper.INSTANCE;
      }
      try {
        Map<String, Object> record = mapper.readValue(line, LinkedHashMap.class);
        return Result.success(record);
      } catch (IOException e) {
        return Result.error(e.getMessage());
      }
    }
  }

}
