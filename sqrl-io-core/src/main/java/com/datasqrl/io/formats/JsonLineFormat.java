/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.impl.InputPreview;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonLineFormat implements TextLineFormat<JsonLineFormat.Configuration> {

  public static final FileFormat FORMAT = FileFormat.JSON;
  public static final String NAME = "json";

  @Override
  public Parser getParser(Configuration config) {
    return new JsonLineParser();
  }

  @Override
  public Configuration getDefaultConfiguration() {
    return new Configuration();
  }

  @Override
  public Writer getWriter(Configuration configuration) {
    return new JsonLineWriter();
  }

  @NoArgsConstructor
  public static class JsonLineParser implements TextLineFormat.Parser {

    private transient ObjectMapper mapper;

    @Override
    public Result parse(@NonNull String line) {
      if (mapper == null) {
        mapper = new ObjectMapper();
      }
      try {
        Map<String, Object> record = mapper.readValue(line, LinkedHashMap.class);
        return Result.success(record);
      } catch (IOException e) {
        return Result.error(e.getMessage());
      }
    }
  }

  @NoArgsConstructor
  public static class JsonLineWriter implements TextLineFormat.Writer {

    private transient ObjectMapper mapper;


  }


  @NoArgsConstructor
  @ToString
  @Builder
  @Getter
  @JsonSerialize
  public static class Configuration implements FormatConfiguration {

    @Override
    public boolean initialize(InputPreview preview, ErrorCollector errors) {
      return true;
    }

    @Override
    public FileFormat getFileFormat() {
      return FORMAT;
    }

    @Override
    public Format getImplementation() {
      return new JsonLineFormat();
    }

    @Override
    public String getName() {
      return NAME;
    }
  }

}
