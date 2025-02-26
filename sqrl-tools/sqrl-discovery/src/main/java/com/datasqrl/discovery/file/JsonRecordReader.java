package com.datasqrl.discovery.file;

import com.datasqrl.graphql.SqrlObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

@AutoService(RecordReader.class)
@Slf4j
public class JsonRecordReader implements RecordReader {

  private final ObjectMapper mapper = SqrlObjectMapper.mapper;

  @Override
  public String getFormat() {
    return "flexible-json";
  }

  @Override
  public Stream<Map<String, Object>> read(InputStream input) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    return reader
        .lines()
        .flatMap(
            line -> {
              if (Strings.isNullOrEmpty(line)) return Stream.of();
              try {
                return Stream.of((Map<String, Object>) mapper.readValue(line, LinkedHashMap.class));
              } catch (IOException e) {
                log.warn("Could not read json record: {}", line);
                return Stream.of();
              }
            });
  }

  @Override
  public Set<String> getExtensions() {
    return Set.of("jsonl");
  }
}
