/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  private final ObjectMapper mapper = SqrlObjectMapper.MAPPER;

  @Override
  public String getFormat() {
    return "flexible-json";
  }

  @Override
  public Stream<Map<String, Object>> read(InputStream input) throws IOException {
    var reader = new BufferedReader(new InputStreamReader(input));
    return reader
        .lines()
        .flatMap(
            line -> {
              if (Strings.isNullOrEmpty(line)) {
                return Stream.of();
              }
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
