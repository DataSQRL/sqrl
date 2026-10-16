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

  public static final Set<String> EXTENSIONS = Set.of("csv");

  private static final String[] DELIMITER_CANDIDATES = new String[] {",", ";", "\t"};

  private static final int MAX_HEADER_LINE_IN_ERROR = 200;

  // Discovery always reads the first line as the header, so the source must skip it as well
  @Override
  public String getFormat() {
    return "flexible-csv";
  }

  @Override
  public Stream<Map<String, Object>> read(InputStream input) throws IOException {
    var reader = new BufferedReader(new InputStreamReader(input));
    var headerLine = reader.readLine();
    if (headerLine == null) {
      return Stream.of();
    }

    var config = inferConfig(headerLine);
    var format = config.format();
    var header = config.header();

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

  @Override
  public Set<String> getExtensions() {
    return EXTENSIONS;
  }

  @Override
  public Map<String, String> getDefaultFormatOptions() {
    return Map.of("flexible-csv.skip-header", "true");
  }

  private static CSVFormat getDefaultFormat(String delimiter) {
    return CSVFormat.DEFAULT.builder().setDelimiter(delimiter.charAt(0)).setTrim(true).build();
  }

  private static Config inferConfig(String headerLine) throws IOException {
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
      var header =
          parser.stream()
              .findFirst()
              .map(r -> r.stream().toArray(String[]::new))
              .orElse(new String[0]);
      var isValidHeader =
          header.length > 0
              && Arrays.stream(header)
                  .allMatch(
                      col -> !Strings.isNullOrEmpty(col) && Character.isLetter(col.charAt(0)));
      if (!isValidHeader) {
        throw new InvalidDataFileException(
            "The first line must be a header row of column names that each start with a letter,"
                + " but was: "
                + StringUtils.abbreviate(headerLine, MAX_HEADER_LINE_IN_ERROR));
      }
      return new Config(format, header);
    }
  }

  private record Config(CSVFormat format, String[] header) {}
}
