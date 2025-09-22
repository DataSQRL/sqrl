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

import com.google.auto.service.AutoService;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
@AutoService(RecordReader.class)
public class CSVRecordReader implements RecordReader {

  public static final String DEFAULT_DELIMITER = ",";
  public static final String DEFAULT_COMMENT = "#";

  public static final Set<String> EXTENSIONS = Set.of("csv");

  private static final String[] DELIMITER_CANDIDATES = new String[] {",", ";", "\t"};

  @Override
  public String getFormat() {
    return "csv";
  }

  @Override
  public Stream<Map<String, Object>> read(InputStream input) throws IOException {
    var reader = new BufferedReader(new InputStreamReader(input));
    var headerLine = reader.readLine();
    var configOpt = inferConfig(headerLine);
    if (configOpt.isEmpty()) {
      return Stream.of();
    }
    var format = configOpt.get().format();
    var header = configOpt.get().header();

    var parser = CSVParser.parse(reader, format);
    return parser.stream()
        .flatMap(
            record -> {
              if (record.size() > header.length) {
                log.info("Skipped record because it does not match header: {}", record);
                return Stream.of();
              }
              var map = new LinkedHashMap<String, Object>(record.size());
              for (var i = 0; i < record.size(); i++) {
                map.put(header[i], record.get(i));
              }
              return Stream.of(map);
            });
  }

  private static boolean isComment(String line, String commentPrefix) {
    return !Strings.isNullOrEmpty(commentPrefix) && line.startsWith(commentPrefix);
  }

  @Override
  public Set<String> getExtensions() {
    return EXTENSIONS;
  }

  private static CSVFormat getDefaultFormat(String delimiter) {
    return org.apache.commons.csv.CSVFormat.DEFAULT
        .builder()
        .setDelimiter(delimiter.charAt(0))
        .setTrim(true)
        .build();
  }

  private static Optional<Config> inferConfig(String headerLine) throws IOException {
    var delimiter = DEFAULT_DELIMITER;
    var topScoringDelimiter =
        Arrays.stream(DELIMITER_CANDIDATES)
            .map(del -> Pair.of(del, StringUtils.countMatches(headerLine, del)))
            .max(Comparator.comparing(Pair<String, Integer>::getValue))
            .get();
    if (topScoringDelimiter.getValue() > 0) {
      delimiter = topScoringDelimiter.getKey();
    }
    var format = getDefaultFormat(delimiter);

    try (var parser = CSVParser.parse(headerLine, format)) {
      Optional<String[]> header =
          parser.stream()
              .findFirst()
              .flatMap(
                  r -> {
                    if (r.size() == 0) {
                      return Optional.empty();
                    }
                    // Make sure all column names are valid
                    if (!r.stream()
                        .allMatch(
                            col ->
                                !Strings.isNullOrEmpty(col) && Character.isLetter(col.charAt(0)))) {
                      log.error("CSV header column names are invalid: {}", r);
                      return Optional.empty();
                    }
                    return Optional.of(r.stream().toArray(String[]::new));
                  });
      if (header.isPresent()) {
        return Optional.of(new Config(format, header.get()));
      }
    }
    return Optional.empty();
  }

  private record Config(CSVFormat format, String[] header) {}
}
